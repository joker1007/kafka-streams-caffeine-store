package com.github.joker1007.kafka.streams.state;

import com.github.benmanes.caffeine.cache.Cache;
import java.util.NoSuchElementException;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class CacheEntryIterator<K extends Comparable<K>, V> implements KeyValueIterator<K, V> {
  private final Cache<K, V> cache;
  private final PeekingIterator<K> cachedKeyIterator;
  private final KeyValueIterator<K, V> storeIterator;
  private final boolean forward;

  public CacheEntryIterator(
      Cache<K, V> cache,
      PeekingIterator<K> cachedKeyIterator,
      KeyValueIterator<K, V> storeIterator,
      boolean forward) {
    this.cache = cache;
    this.cachedKeyIterator = cachedKeyIterator;
    this.storeIterator = storeIterator;
    this.forward = forward;
  }

  @Override
  public void close() {
    storeIterator.close();
  }

  @Override
  public K peekNextKey() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final K nextCacheKey = cachedKeyIterator.hasNext() ? cachedKeyIterator.peek() : null;
    final K nextStoreKey = storeIterator.hasNext() ? storeIterator.peekNextKey() : null;

    if (nextCacheKey == null) {
      return nextStoreKey;
    }

    if (nextStoreKey == null) {
      return nextCacheKey;
    }

    final int comparison = compare(nextCacheKey, nextStoreKey);
    return chooseNextKey(nextCacheKey, nextStoreKey, comparison);
  }

  @Override
  public boolean hasNext() {
    return cachedKeyIterator.hasNext() || storeIterator.hasNext();
  }

  @Override
  public KeyValue<K, V> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final K nextCacheKey = cachedKeyIterator.hasNext() ? cachedKeyIterator.peek() : null;
    final K nextStoreKey = storeIterator.hasNext() ? storeIterator.peekNextKey() : null;

    if (nextCacheKey == null) {
      return nextStoreValue(nextStoreKey);
    }

    if (nextStoreKey == null) {
      return nextCacheValue(nextCacheKey);
    }

    final int comparison = compare(nextCacheKey, nextStoreKey);
    return chooseNextValue(nextCacheKey, nextStoreKey, comparison);
  }

  private KeyValue<K, V> nextStoreValue(final K nextStoreKey) {
    final KeyValue<K, V> next = storeIterator.next();

    if (!next.key.equals(nextStoreKey)) {
      throw new IllegalStateException(
          "Next record key is not the peeked key value; this should not happen");
    }

    return next;
  }

  private KeyValue<K, V> nextCacheValue(final K nextCacheKey) {
    final K nextKey = cachedKeyIterator.next();
    V value = cache.getIfPresent(nextKey);

    if (value == null) {
      throw new IllegalStateException(
          "Next record is not the peeked key value; this should not happen");
    }

    return KeyValue.pair(nextKey, value);
  }

  public int compare(final K cacheKey, final K storeKey) {
    return cacheKey.compareTo(storeKey);
  }

  private KeyValue<K, V> chooseNextValue(
      final K nextCacheKey, final K nextStoreKey, final int comparison) {
    if (forward) {
      if (comparison > 0) {
        return nextStoreValue(nextStoreKey);
      } else if (comparison < 0) {
        return nextCacheValue(nextCacheKey);
      } else {
        // skip the same keyed element
        storeIterator.next();
        return nextCacheValue(nextCacheKey);
      }
    } else {
      if (comparison < 0) {
        return nextStoreValue(nextStoreKey);
      } else if (comparison > 0) {
        return nextCacheValue(nextCacheKey);
      } else {
        // skip the same keyed element
        storeIterator.next();
        return nextCacheValue(nextCacheKey);
      }
    }
  }

  private K chooseNextKey(final K nextCacheKey, final K nextStoreKey, final int comparison) {
    if (forward) {
      if (comparison > 0) {
        return nextStoreKey;
      } else if (comparison < 0) {
        return nextCacheKey;
      } else {
        // skip the same keyed element
        storeIterator.next();
        return nextCacheKey;
      }
    } else {
      if (comparison < 0) {
        return nextStoreKey;
      } else if (comparison > 0) {
        return nextCacheKey;
      } else {
        // skip the same keyed element
        storeIterator.next();
        return nextCacheKey;
      }
    }
  }
}
