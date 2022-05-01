package com.github.joker1007.kafka.streams.state;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

public class CaffeineCachedKeyValueStore<K extends Comparable<K>, V>
    extends WrappedStateStore<KeyValueStore<K, V>, K, V> implements KeyValueStore<K, V> {
  private final Caffeine<Object, Object> caffeine;
  private Cache<K, V> cache;
  private SortedSet<K> cachedKeys;

  public CaffeineCachedKeyValueStore(
      Caffeine<Object, Object> caffeine, KeyValueStore<K, V> wrapped) {
    super(wrapped);
    this.caffeine = caffeine;
  }

  SortedSet<K> getCachedKeys() {
    return cachedKeys;
  }

  Cache<K, V> getCache() {
    return cache;
  }

  private void putValueToCache(K key, V value) {
    cache.put(key, value);
    cachedKeys.add(key);
  }

  @Override
  public void init(StateStoreContext context, StateStore root) {
    super.init(context, root);
    this.cache = caffeine.build();
    this.cachedKeys = new TreeSet<>();
  }

  @Override
  public void put(K key, V value) {
    putValueToCache(key, value);
    wrapped().put(key, value);
  }

  @Override
  public V putIfAbsent(K key, V value) {
    V current = cache.getIfPresent(key);
    if (current == null) {
      putValueToCache(key, value);
    }
    return wrapped().putIfAbsent(key, value);
  }

  @Override
  public void putAll(List<KeyValue<K, V>> entries) {
    entries.forEach(keyValue -> putValueToCache(keyValue.key, keyValue.value));
    wrapped().putAll(entries);
  }

  @Override
  public V delete(K key) {
    cache.invalidate(key);
    cachedKeys.remove(key);
    return wrapped().delete(key);
  }

  @Override
  public V get(K key) {
    return cache.get(
        key,
        k -> {
          cachedKeys.add(k);
          return wrapped().get(k);
        });
  }

  @Override
  public KeyValueIterator<K, V> range(K from, K to) {
    return wrapped().range(from, to);
  }

  @Override
  public KeyValueIterator<K, V> all() {
    return wrapped().all();
  }

  @Override
  public long approximateNumEntries() {
    return wrapped().approximateNumEntries();
  }
}
