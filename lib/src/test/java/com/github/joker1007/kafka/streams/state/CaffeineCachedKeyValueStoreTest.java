package com.github.joker1007.kafka.streams.state;

import static org.junit.jupiter.api.Assertions.*;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CaffeineCachedKeyValueStoreTest {
  private MockProcessorContext<String, String> context;
  private CaffeineCachedKeyValueStore<String, String> caffeineCachedStore;

  @BeforeEach
  void setUp() {
    this.context = new MockProcessorContext<>();
    var store =
        Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("inmemory-key-value-store"),
                Serdes.String(),
                Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();

    var caffeine = Caffeine.newBuilder().maximumSize(100);
    caffeineCachedStore = new CaffeineCachedKeyValueStore<>(caffeine, store);
    context.addStateStore(caffeineCachedStore);
    caffeineCachedStore.init(context.getStateStoreContext(), caffeineCachedStore);
  }

  @Test
  void testPut() {
    caffeineCachedStore.put("foo", "bar");
    assertEquals("bar", caffeineCachedStore.get("foo"));
    assertEquals("bar", caffeineCachedStore.getCache().getIfPresent("foo"));
    assertEquals("bar", caffeineCachedStore.wrapped().get("foo"));
    assertTrue(caffeineCachedStore.getCachedKeys().contains("foo"));
  }
}
