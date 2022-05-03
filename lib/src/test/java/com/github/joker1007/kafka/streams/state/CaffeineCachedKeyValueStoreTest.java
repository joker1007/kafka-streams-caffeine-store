package com.github.joker1007.kafka.streams.state;

import static org.junit.jupiter.api.Assertions.*;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CaffeineCachedKeyValueStoreTest {

  private MockProcessorContext<String, String> context;
  private KeyValueStore<String, String> innerStore;
  private CaffeineCachedKeyValueStore<String, String> caffeineCachedStore;

  @BeforeEach
  void setUp() {
    context = new MockProcessorContext<>();
    innerStore =
        Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("inmemory-key-value-store"),
                Serdes.String(),
                Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();

    var caffeine = Caffeine.newBuilder().maximumSize(10);
    caffeineCachedStore = new CaffeineCachedKeyValueStore<>(caffeine, innerStore, false);
    context.addStateStore(caffeineCachedStore);
    caffeineCachedStore.init(context.getStateStoreContext(), caffeineCachedStore);
  }

  private <K extends Comparable<K>, V> void assertCaching(
      CaffeineCachedKeyValueStore<K, V> caffeineCachedStore, K key, V value) {
    assertEquals(value, caffeineCachedStore.get(key));
    assertEquals(value, caffeineCachedStore.getCache().getIfPresent(key));
    assertEquals(value, caffeineCachedStore.wrapped().get(key));
    assertTrue(caffeineCachedStore.getCachedKeys().contains(key));
  }

  @Test
  void testLoadAllOnInit() {
    innerStore.put("foo1", "bar1");
    innerStore.put("foo2", "bar2");
    innerStore.put("foo3", "bar3");
    innerStore.put("foo4", "bar4");

    var caffeine = Caffeine.newBuilder().maximumSize(10);
    var loadingCaffeineCachedStore = new CaffeineCachedKeyValueStore<>(caffeine, innerStore, true);
    context.addStateStore(caffeineCachedStore);
    loadingCaffeineCachedStore.init(context.getStateStoreContext(), caffeineCachedStore);

    assertCaching(loadingCaffeineCachedStore, "foo1", "bar1");
    assertCaching(loadingCaffeineCachedStore, "foo2", "bar2");
    assertCaching(loadingCaffeineCachedStore, "foo3", "bar3");
    assertCaching(loadingCaffeineCachedStore, "foo4", "bar4");
  }

  @Test
  void testPut() {
    caffeineCachedStore.put("foo", "bar");
    caffeineCachedStore.flush();
    assertCaching(caffeineCachedStore, "foo", "bar");
  }

  @Test
  void testPutIfAbsent() {
    var result = caffeineCachedStore.putIfAbsent("foo", "bar");
    caffeineCachedStore.flush();
    assertEquals("bar", result);
    assertCaching(caffeineCachedStore, "foo", "bar");

    result = caffeineCachedStore.putIfAbsent("foo", "hoge");
    assertEquals("bar", result);
    assertCaching(caffeineCachedStore, "foo", "bar");
  }

  @Test
  void testPutAll() {
    var entries =
        List.of(
            KeyValue.pair("foo1", "bar1"),
            KeyValue.pair("foo2", "bar2"),
            KeyValue.pair("foo3", "bar3"));
    caffeineCachedStore.putAll(entries);
    caffeineCachedStore.flush();

    assertCaching(caffeineCachedStore, "foo1", "bar1");
    assertCaching(caffeineCachedStore, "foo2", "bar2");
    assertCaching(caffeineCachedStore, "foo3", "bar3");
  }

  @Test
  void testDelete() {
    caffeineCachedStore.put("foo", "bar");
    caffeineCachedStore.flush();
    caffeineCachedStore.delete("foo");
    caffeineCachedStore.flush();
    assertNull(caffeineCachedStore.get("foo"));
    assertFalse(caffeineCachedStore.getCachedKeys().contains("foo"));
  }

  @Test
  void testGetWithoutCachedValue() {
    assertNull(caffeineCachedStore.get("foo"));
    assertTrue(caffeineCachedStore.getCachedKeys().isEmpty());
    innerStore.put("foo", "bar");

    assertEquals("bar", caffeineCachedStore.get("foo"));
    assertCaching(caffeineCachedStore, "foo", "bar");
  }

  private void setUpIteratorTest() {
    var entries =
        List.of(
            KeyValue.pair("foo1", "bar1"),
            KeyValue.pair("foo2", "bar2"),
            KeyValue.pair("foo6", "bar6"));
    caffeineCachedStore.putAll(entries);
    caffeineCachedStore.flush();

    caffeineCachedStore.put("foo7", "bar7");

    var entriesOnlyInnerStore =
        List.of(KeyValue.pair("foo3", "bar3"), KeyValue.pair("foo4", "bar4"));
    innerStore.putAll(entriesOnlyInnerStore);
  }

  @Test
  void testAll() {
    setUpIteratorTest();

    try (var it = caffeineCachedStore.all()) {
      List<KeyValue<String, String>> list = new ArrayList<>();
      it.forEachRemaining(list::add);
      assertEquals(6, list.size());
      assertEquals("bar1", list.get(0).value);
      assertEquals("bar2", list.get(1).value);
      assertEquals("bar3", list.get(2).value);
      assertEquals("bar4", list.get(3).value);
      assertEquals("bar6", list.get(4).value);
      assertEquals("bar7", list.get(5).value);
    }
  }

  @Test
  void testAllOnlyCached() {
    setUpIteratorTest();

    try (var it = caffeineCachedStore.allOnlyCached()) {
      List<KeyValue<String, String>> list = new ArrayList<>();
      it.forEachRemaining(list::add);
      assertEquals(4, list.size());
      assertEquals("bar1", list.get(0).value);
      assertEquals("bar2", list.get(1).value);
      assertEquals("bar6", list.get(2).value);
      assertEquals("bar7", list.get(3).value);
    }
  }

  @Test
  void testRange() {
    setUpIteratorTest();

    try (var it = caffeineCachedStore.range("foo2", "foo4")) {
      List<KeyValue<String, String>> list = new ArrayList<>();
      it.forEachRemaining(list::add);
      assertEquals(3, list.size());
      assertEquals("bar2", list.get(0).value);
      assertEquals("bar3", list.get(1).value);
      assertEquals("bar4", list.get(2).value);
    }
  }

  @Test
  void testRangeOnlyCached() {
    setUpIteratorTest();

    try (var it = caffeineCachedStore.rangeOnlyCached("foo2", "foo4")) {
      List<KeyValue<String, String>> list = new ArrayList<>();
      it.forEachRemaining(list::add);
      assertEquals(1, list.size());
      assertEquals("bar2", list.get(0).value);
    }
  }

  @Test
  void testReverseAll() {
    setUpIteratorTest();

    try (var it = caffeineCachedStore.reverseAll()) {
      List<KeyValue<String, String>> list = new ArrayList<>();
      it.forEachRemaining(list::add);
      assertEquals(6, list.size());
      assertEquals("bar7", list.get(0).value);
      assertEquals("bar6", list.get(1).value);
      assertEquals("bar4", list.get(2).value);
      assertEquals("bar3", list.get(3).value);
      assertEquals("bar2", list.get(4).value);
      assertEquals("bar1", list.get(5).value);
    }
  }

  @Test
  void testReverseAllOnlyCached() {
    setUpIteratorTest();

    try (var it = caffeineCachedStore.reverseAllOnlyCached()) {
      List<KeyValue<String, String>> list = new ArrayList<>();
      it.forEachRemaining(list::add);
      assertEquals(4, list.size());
      assertEquals("bar7", list.get(0).value);
      assertEquals("bar6", list.get(1).value);
      assertEquals("bar2", list.get(2).value);
      assertEquals("bar1", list.get(3).value);
    }
  }

  @Test
  void testReverseRange() {
    setUpIteratorTest();

    try (var it = caffeineCachedStore.reverseRange("foo2", "foo4")) {
      List<KeyValue<String, String>> list = new ArrayList<>();
      it.forEachRemaining(list::add);
      assertEquals(3, list.size());
      assertEquals("bar4", list.get(0).value);
      assertEquals("bar3", list.get(1).value);
      assertEquals("bar2", list.get(2).value);
    }
  }

  @Test
  void testReverseRangeOnlyCached() {
    setUpIteratorTest();

    try (var it = caffeineCachedStore.reverseRangeOnlyCached("foo2", "foo7")) {
      List<KeyValue<String, String>> list = new ArrayList<>();
      it.forEachRemaining(list::add);
      assertEquals(3, list.size());
      assertEquals("bar7", list.get(0).value);
      assertEquals("bar6", list.get(1).value);
      assertEquals("bar2", list.get(2).value);
    }
  }

  @Test
  void testPrefixScan() {
    setUpIteratorTest();
    caffeineCachedStore.put("foo21", "bar21");
    caffeineCachedStore.put("foo22", "bar22");
    caffeineCachedStore.put("foo2_1", "bar2_1");
    innerStore.put("foo21", "bar21");
    innerStore.put("foo2_2", "bar2_2");

    try (var it = caffeineCachedStore.prefixScan("foo2", Serdes.String().serializer())) {
      List<KeyValue<String, String>> list = new ArrayList<>();
      it.forEachRemaining(list::add);
      assertEquals(5, list.size());
      assertEquals("bar2", list.get(0).value);
      assertEquals("bar21", list.get(1).value);
      assertEquals("bar22", list.get(2).value);
      assertEquals("bar2_1", list.get(3).value);
      assertEquals("bar2_2", list.get(4).value);
    }
  }

  @Test
  void testPrefixScanOnlyCached() {
    setUpIteratorTest();
    caffeineCachedStore.put("foo21", "bar21");
    caffeineCachedStore.put("foo22", "bar22");
    caffeineCachedStore.put("foo2_1", "bar2_1");
    innerStore.put("foo21", "bar21");
    innerStore.put("foo2_2", "bar2_2");

    try (var it = caffeineCachedStore.prefixScanOnlyCached("foo2", Serdes.String().serializer())) {
      List<KeyValue<String, String>> list = new ArrayList<>();
      it.forEachRemaining(list::add);
      assertEquals(4, list.size());
      assertEquals("bar2", list.get(0).value);
      assertEquals("bar21", list.get(1).value);
      assertEquals("bar22", list.get(2).value);
      assertEquals("bar2_1", list.get(3).value);
    }
  }

  @Test
  void testEviction() {
    IntStream.rangeClosed(1, 100)
        .forEach(
            i -> {
              String key = "foo" + String.format("%03d", i);
              String value = "bar" + String.format("%03d", i);
              caffeineCachedStore.put(key, value);
            });
    caffeineCachedStore.flush();

    List<KeyValue<String, String>> list = new ArrayList<>();
    caffeineCachedStore.getCache().asMap().forEach((k, v) -> list.add(KeyValue.pair(k, v)));
    assertTrue(list.size() < 100);
    list.clear();

    try (var it = caffeineCachedStore.all()) {
      it.forEachRemaining(list::add);
    }

    assertEquals(100, list.size());

    IntStream.rangeClosed(1, 100)
        .forEach(
            i -> {
              String key = "foo" + String.format("%03d", i);
              String value = "bar" + String.format("%03d", i);
              assertEquals(key, list.get(i - 1).key);
              assertEquals(value, list.get(i - 1).value);
            });
  }

  @Test
  void testFlushListener() {
    Map<String, String> newValues = new HashMap<>();
    caffeineCachedStore.setFlushListener(
        (key, newValue, oldValue, timestamp) -> {
          newValues.put(key, newValue);
        },
        false);

    caffeineCachedStore.put("foo1", "bar1");
    caffeineCachedStore.put("foo2", "bar2");
    caffeineCachedStore.put("foo1", "bar1_2");

    caffeineCachedStore.flush();

    assertEquals("bar1_2", newValues.get("foo1"));
    assertEquals("bar2", newValues.get("foo2"));

    caffeineCachedStore.delete("foo2");
    caffeineCachedStore.flush();

    assertNull(newValues.get("foo2"));
  }

  @Test
  void testFlushListenerWithOldValue() {
    Map<String, List<String>> newValues = new HashMap<>();
    caffeineCachedStore.setFlushListener(
        (key, newValue, oldValue, timestamp) -> {
          List<String> list = new ArrayList<>();
          list.add(newValue);
          list.add(oldValue);
          newValues.put(key, list);
        },
        true);

    caffeineCachedStore.put("foo1", "bar1");
    caffeineCachedStore.put("foo2", "bar2");
    caffeineCachedStore.put("foo1", "bar1_2");

    caffeineCachedStore.flush();

    assertEquals("bar1_2", newValues.get("foo1").get(0));
    assertEquals("bar1", newValues.get("foo1").get(1));
    assertEquals("bar2", newValues.get("foo2").get(0));
    assertNull(newValues.get("foo2").get(1));

    caffeineCachedStore.delete("foo2");
    caffeineCachedStore.flush();

    assertNull(newValues.get("foo2").get(0));
    assertEquals("bar2", newValues.get("foo2").get(1));
  }
}
