package com.github.joker1007.kafka.streams.state;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.CacheFlushListener;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

/*
 * Copyright [2022] Tomohiro Hashidate (joker1007)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class CaffeineCachedKeyValueStore<K extends Comparable<K>, V>
    extends WrappedStateStore<KeyValueStore<K, V>, K, V> implements KeyValueStore<K, V> {
  private final String name;
  private final Caffeine<Object, Object> caffeine;
  private Cache<K, V> cache;
  private NavigableSet<K> cachedKeys;
  private NavigableMap<K, DirtyEntry<V>> dirtyEntries;
  private CacheFlushListener<K, V> cacheFlushListener;
  private boolean sendOldValues;
  private final boolean loadAllOnInit;

  private InternalProcessorContext context;

  public CaffeineCachedKeyValueStore(
      String name,
      Caffeine<Object, Object> caffeine,
      KeyValueStore<K, V> wrapped,
      boolean loadAllOnInit) {
    super(wrapped);
    this.name = name;
    this.caffeine = caffeine;
    this.loadAllOnInit = loadAllOnInit;
  }

  SortedSet<K> getCachedKeys() {
    return cachedKeys;
  }

  Cache<K, V> getCache() {
    return cache;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void init(StateStoreContext context, StateStore root) {
    super.init(context, root);
    if (context instanceof InternalProcessorContext) {
      this.context = (InternalProcessorContext) context;
    }
    this.cachedKeys = new ConcurrentSkipListSet<>();
    //noinspection SuspiciousMethodCalls
    caffeine.evictionListener((key, value, cause) -> cachedKeys.remove(key));
    this.cache = caffeine.build();
    this.dirtyEntries = new ConcurrentSkipListMap<>();

    if (loadAllOnInit) {
      loadAllFromInnerStore();
    }
  }

  private void loadAllFromInnerStore() {
    try (var it = wrapped().all()) {
      it.forEachRemaining(
          keyValue -> {
            cache.put(keyValue.key, keyValue.value);
            cachedKeys.add(keyValue.key);
          });
    }
  }

  @Value
  @Builder(setterPrefix = "set")
  static class DirtyEntry<V> {
    V newValue;
    V oldValue;
    Long timestamp;
    Headers headers;
    Integer partition;
    String topic;
    Long offset;

    public ProcessorRecordContext recordContext() {
      return new ProcessorRecordContext(timestamp, offset, partition, topic, headers);
    }
  }

  @Override
  public void put(K key, V value) {
    cache
        .asMap()
        .compute(
            key,
            (k, v) -> {
              cachedKeys.add(k);

              DirtyEntry.DirtyEntryBuilder<V> dirtyEntryBuilder =
                  DirtyEntry.<V>builder().setNewValue(value).setOldValue(sendOldValues ? v : null);
              if (context != null) {
                dirtyEntryBuilder
                    .setTimestamp(context.timestamp())
                    .setHeaders(context.headers())
                    .setPartition(context.partition())
                    .setTopic(context.topic())
                    .setOffset(context.offset());
              }
              dirtyEntries.put(k, dirtyEntryBuilder.build());

              wrapped().put(k, value);
              return value;
            });
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return cache
        .asMap()
        .computeIfAbsent(
            key,
            k -> {
              cachedKeys.add(k);

              DirtyEntry.DirtyEntryBuilder<V> dirtyEntryBuilder =
                  DirtyEntry.<V>builder().setNewValue(value).setOldValue(null);
              if (context != null) {
                dirtyEntryBuilder
                    .setTimestamp(context.timestamp())
                    .setHeaders(context.headers())
                    .setPartition(context.partition())
                    .setTopic(context.topic())
                    .setOffset(context.offset());
              }
              dirtyEntries.put(k, dirtyEntryBuilder.build());

              wrapped().put(key, value);
              return value;
            });
  }

  @Override
  public void putAll(List<KeyValue<K, V>> entries) {
    entries.forEach(
        keyValue ->
            cache
                .asMap()
                .compute(
                    keyValue.key,
                    (k, v) -> {
                      cachedKeys.add(k);

                      DirtyEntry.DirtyEntryBuilder<V> dirtyEntryBuilder =
                          DirtyEntry.<V>builder()
                              .setNewValue(keyValue.value)
                              .setOldValue(sendOldValues ? v : null);
                      if (context != null) {
                        dirtyEntryBuilder
                            .setTimestamp(context.timestamp())
                            .setHeaders(context.headers())
                            .setPartition(context.partition())
                            .setTopic(context.topic())
                            .setOffset(context.offset());
                      }
                      dirtyEntries.put(k, dirtyEntryBuilder.build());

                      wrapped().put(k, keyValue.value);
                      return keyValue.value;
                    }));
  }

  @Override
  public V delete(K key) {
    V oldValue = get(key);
    cache
        .asMap()
        .compute(
            key,
            (k, v) -> {
              cachedKeys.remove(k);

              DirtyEntry.DirtyEntryBuilder<V> dirtyEntryBuilder =
                  DirtyEntry.<V>builder().setNewValue(null).setOldValue(sendOldValues ? v : null);
              if (context != null) {
                dirtyEntryBuilder
                    .setTimestamp(context.timestamp())
                    .setHeaders(context.headers())
                    .setPartition(context.partition())
                    .setTopic(context.topic())
                    .setOffset(context.offset());
              }
              dirtyEntries.put(k, dirtyEntryBuilder.build());
              wrapped().delete(key);

              return null;
            });

    return oldValue;
  }

  @Override
  public V get(K key) {
    return cache.get(
        key,
        k -> {
          V value = wrapped().get(k);
          if (value != null) {
            cachedKeys.add(k);
          }
          return value;
        });
  }

  @Override
  public KeyValueIterator<K, V> range(K from, K to) {
    var keyRange = cachedKeys.subSet(from, true, to, true);
    return new MergedCacheEntryIterator<>(
        cache, new PeekingIterator<>(keyRange.iterator()), wrapped().range(from, to), true);
  }

  public KeyValueIterator<K, V> rangeOnlyCached(K from, K to) {
    var keyRange = cachedKeys.subSet(from, true, to, true);
    return new CacheEntryIterator<>(cache, new PeekingIterator<>(keyRange.iterator()));
  }

  @Override
  public KeyValueIterator<K, V> reverseRange(K from, K to) {
    var keyRange = cachedKeys.subSet(from, true, to, true);
    return new MergedCacheEntryIterator<>(
        cache,
        new PeekingIterator<>(keyRange.descendingIterator()),
        wrapped().reverseRange(from, to),
        false);
  }

  public KeyValueIterator<K, V> reverseRangeOnlyCached(K from, K to) {
    var keyRange = cachedKeys.subSet(from, true, to, true);
    return new CacheEntryIterator<>(cache, new PeekingIterator<>(keyRange.descendingIterator()));
  }

  @Override
  public KeyValueIterator<K, V> all() {
    return new MergedCacheEntryIterator<>(
        cache, new PeekingIterator<>(cachedKeys.iterator()), wrapped().all(), true);
  }

  public KeyValueIterator<K, V> allOnlyCached() {
    return new CacheEntryIterator<>(cache, new PeekingIterator<>(cachedKeys.iterator()));
  }

  @Override
  public KeyValueIterator<K, V> reverseAll() {
    return new MergedCacheEntryIterator<>(
        cache,
        new PeekingIterator<>(cachedKeys.descendingIterator()),
        wrapped().reverseAll(),
        false);
  }

  public KeyValueIterator<K, V> reverseAllOnlyCached() {
    return new CacheEntryIterator<>(cache, new PeekingIterator<>(cachedKeys.descendingIterator()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(
      P prefix, PS prefixKeySerializer) {
    var keyRange = cachedKeys.subSet((K) prefix, true, succKey(prefix), false);
    return new MergedCacheEntryIterator<>(
        cache,
        new PeekingIterator<>(keyRange.iterator()),
        wrapped().prefixScan(prefix, prefixKeySerializer),
        true);
  }

  @SuppressWarnings("unchecked")
  public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScanOnlyCached(
      P prefix, PS prefixKeySerializer) {
    var keyRange = cachedKeys.subSet((K) prefix, true, succKey(prefix), false);
    return new CacheEntryIterator<>(cache, new PeekingIterator<>(keyRange.iterator()));
  }

  @SuppressWarnings({"unchecked", "WrapperTypeMayBePrimitive"})
  private <P> K succKey(P key) {
    if (key instanceof String) {
      String stringKey = (String) key;
      var lastChar = stringKey.charAt(stringKey.length() - 1);
      var nextChar = lastChar + 1;
      var nextKey = stringKey.substring(0, stringKey.length() - 1) + Character.toString(nextChar);
      return (K) nextKey;
    } else if (key instanceof Integer) {
      Integer next = (Integer) key + 1;
      return (K) next;
    } else if (key instanceof Long) {
      Long next = (Long) key + 1;
      return (K) next;
    } else {
      throw new UnsupportedOperationException("Given key type is not supported");
    }
  }

  @Override
  public long approximateNumEntries() {
    return wrapped().approximateNumEntries();
  }

  @Override
  public boolean setFlushListener(CacheFlushListener<K, V> listener, boolean sendOldValues) {
    this.cacheFlushListener = listener;
    this.sendOldValues = sendOldValues;
    return super.setFlushListener(listener, sendOldValues);
  }

  @Override
  public void flushCache() {
    super.flushCache();
    doFlush();
  }

  @Override
  public void flush() {
    super.flush();
    doFlush();
  }

  private void doFlush() {
    dirtyEntries.forEach(
        (k, v) -> {
          if (cacheFlushListener != null) {
            if (context != null) {
              var current = context.recordContext();
              context.setRecordContext(v.recordContext());
              try {
                cacheFlushListener.apply(
                    k,
                    v.newValue,
                    v.oldValue,
                    v.timestamp != null ? v.timestamp : System.currentTimeMillis());
              } finally {
                context.setRecordContext(current);
              }
            } else {
              cacheFlushListener.apply(k, v.newValue, v.oldValue, System.currentTimeMillis());
            }
          }
        });
    dirtyEntries.clear();
  }
}
