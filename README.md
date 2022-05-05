# kafka-streams-caffeine-store
[![CircleCI](https://circleci.com/gh/joker1007/kafka-streams-caffeine-store/tree/main.svg?style=svg)](https://circleci.com/gh/joker1007/kafka-streams-caffeine-store/tree/main)

This is WrapperStateStore for caching record that is stored in standard StateStore.

The builtin caching stores exist, but those stores need deserialization when an application fetches records.

I want to avoid deserialization and keep object in memory cache.

# Usage

```java
var innerStoreBuilder =
    Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("inner-key-value-store"),
            Serdes.String(),
            Serdes.String());

var caffeine = Caffeine.newBuilder();
var storeBuilder =
    new CaffeineCachedKeyValueStoreBuilder<>(
        "caffeine-cached-store", caffeine, innerStoreBuilder)
        .withLoadAllOnInitEnabled();

Topology toplogy = new StreamsBuilder().build();
topology.addStateStore(storeBuilder);

toplogy.connectProcessorAndStateStores(
  "FooProcessor",
  storeBuilder.name()
);
```


## Contributing

1. Fork it ( https://github.com/joker1007/kafka-streams-caffeine-store/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
