package com.github.joker1007.kafka.streams.state;

import com.github.benmanes.caffeine.cache.Cache;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class CacheEntryIterator<K, V> implements KeyValueIterator<K, V> {
  private final Cache<K, V> cache;
  private final PeekingIterator<K> cachedKeyIterator;
  private final boolean forward;

  public CacheEntryIterator(
      Cache<K, V> cache, PeekingIterator<K> cachedKeyIterator, boolean forward) {
    this.cache = cache;
    this.cachedKeyIterator = cachedKeyIterator;
    this.forward = forward;
  }

  @Override
  public void close() {}

  @Override
  public K peekNextKey() {
    return cachedKeyIterator.peek();
  }

  @Override
  public boolean hasNext() {
    return cachedKeyIterator.hasNext();
  }

  @Override
  public KeyValue<K, V> next() {
    K nextKey = cachedKeyIterator.next();
    V value = cache.getIfPresent(nextKey);
    if (value == null) {
      throw new IllegalStateException("Next record is not found in cache; this should not happen");
    }

    return KeyValue.pair(nextKey, value);
  }
}
