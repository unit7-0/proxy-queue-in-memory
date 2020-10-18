# Topic usage example

## Writing messages
```java
final int partitionsCount = 8;
final var topic = new TopicImpl("topic-name", partitionsCount);

final byte[] payload = new byte[] { 1, 2, 3 };
topic.writeMessage(payload);
```

## Reading messages
```java
final int partitionsCount = 8;
final var topic = new TopicImpl("topic-name", partitionsCount);
final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(topic.partitionsCount());

final var consumerList = List.of(...);

topic.assignPartitions(consumerList);
for (ServiceDataDeliveryConsumer consumer : consumerList) {
    executorService.scheduleAtFixedRate(consumer, 0, 1, TimeUnit.SECONDS);
}
```

# Running application
To run application you can use maven command: `mvn exec:java`.

Before starting you need to up proxying backend service, default uri is `http://localhost:8081` that could be
changed in `AppConfig` file. 
You can use simple echo server like this https://gist.github.com/huyng/814831 for example.
 