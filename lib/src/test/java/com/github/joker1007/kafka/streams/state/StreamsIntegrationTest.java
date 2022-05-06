package com.github.joker1007.kafka.streams.state;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

public class StreamsIntegrationTest {
  @Test
  void testStreamsIntegration() {
    var builder = new StreamsBuilder();

    var innerStoreBuilder =
        Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("inmemory-key-value-store"),
                Serdes.String(),
                Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled();

    final String STORE_NAME = "caffeine-cached-store";
    var caffeine = Caffeine.newBuilder();
    var storeBuilder =
        new CaffeineCachedKeyValueStoreBuilder<>(STORE_NAME, caffeine, innerStoreBuilder)
            .withLoadAllOnInitEnabled();

    Topology topology = builder.build();

    final String SOURCE_NAME = "Source-test-topic";
    final String TOPIC_NAME = "test-topic";
    topology.addStateStore(storeBuilder);
    topology.addSource(
        SOURCE_NAME, Serdes.String().deserializer(), Serdes.String().deserializer(), TOPIC_NAME);
    final String PROCESSOR_NAME = "WriteStore";
    topology.addProcessor(
        PROCESSOR_NAME,
        () ->
            new Processor<String, String, String, String>() {
              private KeyValueStore<String, String> store;

              @Override
              public void init(ProcessorContext<String, String> context) {
                this.store = context.getStateStore(STORE_NAME);
              }

              @Override
              public void process(Record<String, String> record) {
                store.put(record.key(), record.value());
              }
            },
        SOURCE_NAME);
    topology.connectProcessorAndStateStores(PROCESSOR_NAME, storeBuilder.name());

    try (var driver = new TopologyTestDriver(topology)) {
      TestInputTopic<String, String> testInputTopic =
          driver.createInputTopic(
              TOPIC_NAME, Serdes.String().serializer(), Serdes.String().serializer());

      testInputTopic.pipeInput("foo1", "bar1");
      testInputTopic.pipeInput("foo2", "bar2");

      KeyValueStore<String, String> store = driver.getKeyValueStore(STORE_NAME);
      assertEquals("bar1", store.get("foo1"));
      assertEquals("bar2", store.get("foo2"));
    }
  }
}
