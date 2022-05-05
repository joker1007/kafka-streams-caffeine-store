package com.github.joker1007.kafka.streams.state;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Map;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

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
public class CaffeineCachedKeyValueStoreBuilder<K extends Comparable<K>, V>
    implements StoreBuilder<CaffeineCachedKeyValueStore<K, V>> {
  private final String name;
  private final Caffeine<Object, Object> caffeine;
  private final StoreBuilder<KeyValueStore<K, V>> innerStoreBuilder;
  private boolean loadAllOnInit;

  public CaffeineCachedKeyValueStoreBuilder(
      String name,
      Caffeine<Object, Object> caffeine,
      StoreBuilder<KeyValueStore<K, V>> innerStoreBuilder) {
    this.name = name;
    this.caffeine = caffeine;
    this.innerStoreBuilder = innerStoreBuilder;
    this.loadAllOnInit = false;
  }

  public StoreBuilder<CaffeineCachedKeyValueStore<K, V>> withLoadAllOnInitEnabled() {
    this.loadAllOnInit = true;
    return this;
  }

  public StoreBuilder<CaffeineCachedKeyValueStore<K, V>> withLoadAllOnInitDisabled() {
    this.loadAllOnInit = false;
    return this;
  }

  @Override
  public StoreBuilder<CaffeineCachedKeyValueStore<K, V>> withCachingEnabled() {
    throw new UnsupportedOperationException("withCachingEnabled is unsupported");
  }

  @Override
  public StoreBuilder<CaffeineCachedKeyValueStore<K, V>> withCachingDisabled() {
    throw new UnsupportedOperationException("withCachingDisabled is unsupported");
  }

  @Override
  public StoreBuilder<CaffeineCachedKeyValueStore<K, V>> withLoggingEnabled(
      Map<String, String> config) {
    throw new UnsupportedOperationException("withLoggingEnabled is unsupported");
  }

  @Override
  public StoreBuilder<CaffeineCachedKeyValueStore<K, V>> withLoggingDisabled() {
    throw new UnsupportedOperationException("withLoggingDisabled is unsupported");
  }

  @Override
  public CaffeineCachedKeyValueStore<K, V> build() {
    return new CaffeineCachedKeyValueStore<>(caffeine, innerStoreBuilder.build(), loadAllOnInit);
  }

  @Override
  public Map<String, String> logConfig() {
    return innerStoreBuilder.logConfig();
  }

  @Override
  public boolean loggingEnabled() {
    return innerStoreBuilder.loggingEnabled();
  }

  @Override
  public String name() {
    return name;
  }
}
