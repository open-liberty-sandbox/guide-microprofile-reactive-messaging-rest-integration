# feature/my-solution — Change Log

This document summarises the key additions that implement bidirectional MicroProfile Reactive Messaging between the System Microservice and Inventory Microservice via a Kafka message broker, triggered by a REST endpoint.

---

## 1. Publish system load metrics to Kafka (`SystemService.java`)

**Commit:** `feat: emit SystemLoad events on outgoing systemLoad channel`

### What changed

The `sendSystemLoad()` method was added to `SystemService` with the `@Outgoing("systemLoad")` annotation, returning a reactive `Publisher<SystemLoad>`:

```java
@Outgoing("systemLoad")
public Publisher<SystemLoad> sendSystemLoad() {
    return Flowable.interval(15, TimeUnit.SECONDS)
                   .map((interval -> new SystemLoad(getHostname(),
                         OS_MEAN.getSystemLoadAverage())));
}
```

A helper `getHostname()` resolves the container's hostname via `InetAddress.getLocalHost()`, falling back to the `HOSTNAME` environment variable.

### Why

`@Outgoing("systemLoad")` declares that this method produces messages onto a named channel. The MicroProfile Reactive Messaging runtime subscribes to the returned `Publisher` and forwards each emitted item to the configured Kafka connector. Using RxJava3's `Flowable.interval` produces a periodic, back-pressure-aware stream — the runtime is never pushed more items than it can handle.

---

## 2. Consume system load events from Kafka (`InventoryResource.java`)

**Commit:** `feat: add updateStatus to consume incoming systemLoad messages`

### What changed

The `updateStatus()` method was added to `InventoryResource` with the `@Incoming("systemLoad")` annotation:

```java
@Incoming("systemLoad")
public void updateStatus(SystemLoad sl) {
    String hostname = sl.hostname;
    if (manager.getSystem(hostname).isPresent()) {
        manager.updateCpuStatus(hostname, sl.loadAverage);
        logger.info("Host " + hostname + " was updated: " + sl);
    } else {
        manager.addSystem(hostname, sl.loadAverage);
        logger.info("Host " + hostname + " was added: " + sl);
    }
}
```

### Why

`@Incoming("systemLoad")` tells the runtime to call this method for every message arriving on the `systemLoad` channel. The method is a simple void consumer — no manual Kafka polling, offset management, or thread handling is needed.

---

## 3. Add REST endpoint to trigger system property requests (`InventoryResource.java`)

**Commit:** `feat: add PUT /data endpoint to request system properties`

### What changed

The `updateSystemProperty()` method was added as a `@PUT` endpoint at `/inventory/data`. It accepts a property name as plain text and emits it onto a reactive stream via a `FlowableEmitter`:

```java
private FlowableEmitter<String> propertyNameEmitter;

@PUT
@Path("/data")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.TEXT_PLAIN)
public Response updateSystemProperty(String propertyName) {
    logger.info("updateSystemProperty: " + propertyName);
    propertyNameEmitter.onNext(propertyName);
    return Response
             .status(Response.Status.OK)
             .entity("Request successful for the " + propertyName + " property\n")
             .build();
}
```

### Why

A JAX-RS endpoint provides the external entry point for a client to request a specific system property value (e.g., `os.name`). Rather than calling the System Microservice directly, it emits the property name onto the reactive stream — keeping the HTTP layer and the messaging layer decoupled. The `FlowableEmitter` bridges the imperative REST world to the reactive stream.

---

## 4. Publish property name requests to Kafka (`InventoryResource.java`)

**Commit:** `feat: add sendPropertyName to produce outgoing property name requests`

### What changed

The `sendPropertyName()` method was added with the `@Outgoing("requestSystemProperty")` annotation. It creates a `Flowable` backed by a `FlowableEmitter` that is stored as a field so the REST endpoint can push items onto it:

```java
@Outgoing("requestSystemProperty")
public Publisher<String> sendPropertyName() {
    Flowable<String> flowable = Flowable.<String>create(emitter ->
        this.propertyNameEmitter = emitter, BackpressureStrategy.BUFFER);
    return flowable;
}
```

### Why

`Flowable.create` with `BackpressureStrategy.BUFFER` creates a hot stream driven by external calls to `propertyNameEmitter.onNext()`. The emitter reference is captured at startup so that every `PUT /inventory/data` call can inject a property name string into the stream without blocking. The MicroProfile Reactive Messaging runtime picks up each emission and publishes it to the `request.system.property` Kafka topic.

---

## 5. System Microservice acts as a processor (`SystemService.java`)

**Commit:** `feat: add sendProperty processor to consume requests and respond with property values`

### What changed

The `sendProperty()` method was added to `SystemService`, annotated with **both** `@Incoming("propertyRequest")` and `@Outgoing("propertyResponse")`. It receives a property name, reads the JVM property, and returns a `PropertyMessage`:

```java
@Incoming("propertyRequest")
@Outgoing("propertyResponse")
public PropertyMessage sendProperty(String propertyName) {
    logger.info("sendProperty: " + propertyName);
    if (propertyName == null || propertyName.isEmpty()) {
        logger.warning(propertyName == null ? "Null" : "An empty string"
            + " is not System property.");
        return null;
    }
    return new PropertyMessage(getHostname(),
                   propertyName,
                   System.getProperty(propertyName, "unknown"));
}
```

### Why

A method annotated with both `@Incoming` and `@Outgoing` acts as a **processor** — it consumes from one channel and produces to another in a single step. This is the most concise way to express a request/response pattern over Kafka: the runtime handles consuming the inbound message, calling the method, and publishing the return value without any manual producer or consumer boilerplate. Returning `null` signals to the runtime that no outgoing message should be published for that invocation.

---

## 6. Inventory Microservice consumes the property response (`InventoryResource.java`)

**Commit:** `feat: add getPropertyMessage to consume property response from System Microservice`

### What changed

The `getPropertyMessage()` method was added with the `@Incoming("addSystemProperty")` annotation. It receives a `PropertyMessage` and stores the property key/value in `InventoryManager`:

```java
@Incoming("addSystemProperty")
public void getPropertyMessage(PropertyMessage pm) {
    logger.info("getPropertyMessage: " + pm);
    String hostId = pm.hostname;
    if (manager.getSystem(hostId).isPresent()) {
        manager.updatePropertyMessage(hostId, pm.key, pm.value);
        logger.info("Host " + hostId + " was updated: " + pm);
    } else {
        manager.addSystem(hostId, pm.key, pm.value);
        logger.info("Host " + hostId + " was added: " + pm);
    }
}
```

### Why

The Inventory Microservice is both a consumer (system load) and a producer/consumer (property request/response). Separating the `systemLoad` consumer from the `addSystemProperty` consumer keeps each method focused on one concern. The deserialized `PropertyMessage` carries `hostname`, `key`, and `value`, which are stored directly in `InventoryManager`.

---

## 7. Wire all channels to Kafka via MicroProfile Config

**Commit:** `feat: add microprofile-config.properties for all Kafka channel bindings`

### What changed — System Microservice (3 channels)

```properties
# system.load — outbound system load metrics
mp.messaging.connector.liberty-kafka.bootstrap.servers=kafka:9092
mp.messaging.outgoing.systemLoad.connector=liberty-kafka
mp.messaging.outgoing.systemLoad.topic=system.load
mp.messaging.outgoing.systemLoad.value.serializer=io.openliberty.guides.models.SystemLoad$SystemLoadSerializer

# add.system.property — outbound property response
mp.messaging.outgoing.propertyResponse.connector=liberty-kafka
mp.messaging.outgoing.propertyResponse.topic=add.system.property
mp.messaging.outgoing.propertyResponse.value.serializer=io.openliberty.guides.models.PropertyMessage$PropertyMessageSerializer

# request.system.property — inbound property name request
mp.messaging.incoming.propertyRequest.connector=liberty-kafka
mp.messaging.incoming.propertyRequest.topic=request.system.property
mp.messaging.incoming.propertyRequest.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
mp.messaging.incoming.propertyRequest.group.id=property-name
```

### What changed — Inventory Microservice (3 channels)

```properties
# system.load — inbound system load metrics
mp.messaging.connector.liberty-kafka.bootstrap.servers=kafka:9092
mp.messaging.incoming.systemLoad.connector=liberty-kafka
mp.messaging.incoming.systemLoad.topic=system.load
mp.messaging.incoming.systemLoad.value.deserializer=io.openliberty.guides.models.SystemLoad$SystemLoadDeserializer
mp.messaging.incoming.systemLoad.group.id=system-load-status

# add.system.property — inbound property response
mp.messaging.incoming.addSystemProperty.connector=liberty-kafka
mp.messaging.incoming.addSystemProperty.topic=add.system.property
mp.messaging.incoming.addSystemProperty.value.deserializer=io.openliberty.guides.models.PropertyMessage$PropertyMessageDeserializer
mp.messaging.incoming.addSystemProperty.group.id=sys-property

# request.system.property — outbound property name request
mp.messaging.outgoing.requestSystemProperty.connector=liberty-kafka
mp.messaging.outgoing.requestSystemProperty.topic=request.system.property
mp.messaging.outgoing.requestSystemProperty.value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

### Why

Three Kafka topics coordinate the two-way flow:

| Topic                     | Direction          | Payload           |
| ------------------------- | ------------------ | ----------------- |
| `system.load`             | System → Inventory | `SystemLoad`      |
| `request.system.property` | Inventory → System | `String` (key)    |
| `add.system.property`     | System → Inventory | `PropertyMessage` |

Logical channel names in annotations (`systemLoad`, `requestSystemProperty`, `propertyRequest`, `propertyResponse`, `addSystemProperty`) are decoupled from physical topic names — all binding is in config, not Java source.

---

## 8. Add `PropertyMessage` model (`PropertyMessage.java`)

**Commit:** `feat: add PropertyMessage model with JSONB serializer and deserializer`

### What changed

A new model class `PropertyMessage` was added alongside `SystemLoad` in the `models` module:

```java
public class PropertyMessage {
    public String hostname;
    public String key;
    public String value;

    public static class PropertyMessageSerializer implements Serializer<Object> {
        @Override
        public byte[] serialize(String topic, Object data) {
            return JSONB.toJson(data).getBytes();
        }
    }

    public static class PropertyMessageDeserializer implements Deserializer<PropertyMessage> {
        @Override
        public PropertyMessage deserialize(String topic, byte[] data) {
            if (data == null) return null;
            return JSONB.fromJson(new String(data), PropertyMessage.class);
        }
    }
}
```

### Why

`PropertyMessage` carries three fields: which host the property was read from (`hostname`), which property was requested (`key`), and its runtime value (`value`). Placing the serializer and deserializer as inner classes mirrors the pattern established by `SystemLoad`, keeping serialization logic co-located with the model and both services referencing the same inner class paths in config.

---

## 9. Create the integration test (`InventoryServiceIT.java`)

**Commit:** `feat: create InventoryServiceIT integration test covering both messaging flows`

### What changed

`InventoryServiceIT.java` tests both the system load flow and the property request flow using Testcontainers:

**Test 1 — `testCpuUsage()`:** Injects a `SystemLoad` message directly via a `KafkaProducer`, waits 5 s, then queries `GET /inventory/systems` and asserts the values match.

```java
@Test
public void testCpuUsage() throws InterruptedException {
    SystemLoad sl = new SystemLoad("localhost", 1.1);
    producer.send(new ProducerRecord<String, SystemLoad>("system.load", sl));
    Thread.sleep(5000);
    Response response = client.getSystems();
    List<Properties> systems =
        response.readEntity(new GenericType<List<Properties>>() { });
    assertEquals(200, response.getStatus(), "Response should be 200");
    assertEquals(systems.size(), 1);
    // assert hostname and systemLoad fields...
}
```

**Test 2 — `testGetProperty()`:** Calls `PUT /inventory/data` with `"os.name"`, then polls the `request.system.property` Kafka topic via a `KafkaConsumer` to assert the property name was published:

```java
@Test
public void testGetProperty() {
    Response response = client.updateSystemProperty("os.name");
    assertEquals(200, response.getStatus(), "Response should be 200");
    ConsumerRecords<String, String> records =
        propertyConsumer.poll(Duration.ofMillis(4000));
    assertTrue(records.count() > 0, "No records polled");
    for (ConsumerRecord<String, String> record : records) {
        assertEquals("os.name", record.value());
    }
}
```

### Why

The two tests cover the two independent message flows. `testGetProperty()` verifies the REST-to-Kafka path at the boundary the Inventory Microservice owns — it confirms the property name was emitted to `request.system.property` without needing the System Microservice container to be running. The `propertyConsumer` is set up in `@BeforeEach` subscribed to `request.system.property` so it is positioned to catch the record emitted by `PUT /inventory/data`.
