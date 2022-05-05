package com.github.joker1007.kafka.streams.state;

import com.github.benmanes.caffeine.cache.Cache;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

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
