package com.github.joker1007.kafka.streams.state;

import com.github.benmanes.caffeine.cache.Cache;
import java.util.NoSuchElementException;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

/*
 * This class is based on AbstractMergedSortedCacheStoreIterator of Apache Kafka
 *
 * Copyright [2022] Tomohiro Hashidate (joker1007)
 * Copyright [2022] Apache Software Foundation
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

public class MergedCacheEntryIterator<K extends Comparable<K>, V>
    implements KeyValueIterator<K, V> {
  private final Cache<K, V> cache;
  private final PeekingIterator<K> cachedKeyIterator;
  private final KeyValueIterator<K, V> storeIterator;
  private final boolean forward;

  public MergedCacheEntryIterator(
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
      return nextCacheValue();
    }

    final int comparison = compare(nextCacheKey, nextStoreKey);
    return chooseNextValue(nextStoreKey, comparison);
  }

  private KeyValue<K, V> nextStoreValue(final K nextStoreKey) {
    final KeyValue<K, V> next = storeIterator.next();

    if (!next.key.equals(nextStoreKey)) {
      throw new IllegalStateException(
          "Next record key is not the peeked key value; this should not happen");
    }

    return next;
  }

  private KeyValue<K, V> nextCacheValue() {
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

  private KeyValue<K, V> chooseNextValue(final K nextStoreKey, final int comparison) {
    if (forward) {
      if (comparison > 0) {
        return nextStoreValue(nextStoreKey);
      } else if (comparison < 0) {
        return nextCacheValue();
      } else {
        // skip the same keyed element
        storeIterator.next();
        return nextCacheValue();
      }
    } else {
      if (comparison < 0) {
        return nextStoreValue(nextStoreKey);
      } else if (comparison > 0) {
        return nextCacheValue();
      } else {
        // skip the same keyed element
        storeIterator.next();
        return nextCacheValue();
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
