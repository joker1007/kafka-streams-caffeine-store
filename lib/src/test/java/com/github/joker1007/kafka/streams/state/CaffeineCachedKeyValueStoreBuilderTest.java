package com.github.joker1007.kafka.streams.state;

import static org.junit.jupiter.api.Assertions.*;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

class CaffeineCachedKeyValueStoreBuilderTest {

  private <K extends Comparable<K>, V> void assertCaching(
      CaffeineCachedKeyValueStore<K, V> caffeineCachedStore, K key, V value) {
    assertEquals(value, caffeineCachedStore.get(key));
    assertEquals(value, caffeineCachedStore.getCache().getIfPresent(key));
    assertEquals(value, caffeineCachedStore.wrapped().get(key));
    assertTrue(caffeineCachedStore.getCachedKeys().contains(key));
  }

  @Test
  void testBuildingStore() {
    MockProcessorContext<String, String> context = new MockProcessorContext<>();
    var innerStoreBuilder =
        Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("inmemory-key-value-store"),
                Serdes.String(),
                Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled();

    var caffeine = Caffeine.newBuilder().maximumSize(10);
    var storeBuilder =
        new CaffeineCachedKeyValueStoreBuilder<>(
            "caffeine-cached-store", caffeine, innerStoreBuilder);
    var store = storeBuilder.build();
    store.init(context.getStateStoreContext(), store);
    context.addStateStore(store);

    store.put("foo", "bar");
    assertCaching(store, "foo", "bar");
  }
}
