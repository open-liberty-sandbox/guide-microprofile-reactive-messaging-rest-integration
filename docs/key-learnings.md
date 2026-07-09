# Key Learnings — Quiz

Test your understanding of the MicroProfile Reactive Messaging REST Integration guide.

---

**Q1. What are the two microservices in this guide and what does each one do?**

<details>
<summary>Answer</summary>

| Service                    | Port | Responsibility                                                                                                                                                   |
| -------------------------- | ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **System Microservice**    | 9083 | Publishes CPU load metrics to Kafka every 15 seconds; also consumes property name requests from Kafka and publishes the resolved property value back to Kafka    |
| **Inventory Microservice** | 9085 | Consumes system load events from Kafka; exposes a REST `PUT /data` endpoint to request system properties; consumes property responses from Kafka and stores them |

</details>

---

**Q2. How many Kafka topics does this guide use, and what is each one for?**

<details>
<summary>Answer</summary>

Three topics:

| Topic                     | Published by           | Consumed by            | Payload                  |
| ------------------------- | ---------------------- | ---------------------- | ------------------------ |
| `system.load`             | System Microservice    | Inventory Microservice | `SystemLoad` (JSON)      |
| `request.system.property` | Inventory Microservice | System Microservice    | `String` (key name)      |
| `add.system.property`     | System Microservice    | Inventory Microservice | `PropertyMessage` (JSON) |

`system.load` carries periodic CPU metrics. `request.system.property` carries a property name requested by a REST client. `add.system.property` carries the resolved property value from the System Microservice back to Inventory.

</details>

---

**Q3. What REST endpoint triggers the property lookup flow, and what happens internally when it is called?**

<details>
<summary>Answer</summary>

`PUT /inventory/data` with the property name as a plain-text request body.

Internally, `InventoryResource.updateSystemProperty()` calls `propertyNameEmitter.onNext(propertyName)`, which pushes the property name into a hot `Flowable` stream. The `@Outgoing("requestSystemProperty")` method (`sendPropertyName()`) is subscribed to that stream and feeds each emitted string to the `liberty-kafka` connector, which publishes it to the `request.system.property` topic.

```java
propertyNameEmitter.onNext(propertyName);
```

</details>

---

**Q4. How does `FlowableEmitter` bridge the REST layer to the reactive messaging layer?**

<details>
<summary>Answer</summary>

`sendPropertyName()` uses `Flowable.create()` to construct a stream and captures the emitter reference as a field:

```java
@Outgoing("requestSystemProperty")
public Publisher<String> sendPropertyName() {
    Flowable<String> flowable = Flowable.<String>create(emitter ->
        this.propertyNameEmitter = emitter, BackpressureStrategy.BUFFER);
    return flowable;
}
```

Because `InventoryResource` is `@ApplicationScoped`, there is exactly one instance and therefore one emitter. Every subsequent `PUT /data` call invokes `propertyNameEmitter.onNext(propertyName)` on that single stored emitter, injecting a value into the already-live stream that the MicroProfile runtime is subscribed to. `BackpressureStrategy.BUFFER` queues items if the downstream is temporarily slow.

</details>

---

**Q5. What does it mean for `SystemService.sendProperty()` to be annotated with both `@Incoming` and `@Outgoing`?**

<details>
<summary>Answer</summary>

A method annotated with both acts as a **processor** — it consumes from one channel and produces to another in a single step:

```java
@Incoming("propertyRequest")
@Outgoing("propertyResponse")
public PropertyMessage sendProperty(String propertyName) {
    return new PropertyMessage(getHostname(), propertyName,
                               System.getProperty(propertyName, "unknown"));
}
```

The MicroProfile Reactive Messaging runtime calls this method for each incoming message, takes the return value, and publishes it to the outgoing channel automatically. Returning `null` signals that nothing should be published for that invocation. No producer boilerplate is needed.

</details>

---

**Q6. How many channel names does `InventoryResource` declare in total, and what are they?**

<details>
<summary>Answer</summary>

Three channel names:

| Annotation                           | Channel name            | Direction |
| ------------------------------------ | ----------------------- | --------- |
| `@Incoming("systemLoad")`            | `systemLoad`            | Incoming  |
| `@Outgoing("requestSystemProperty")` | `requestSystemProperty` | Outgoing  |
| `@Incoming("addSystemProperty")`     | `addSystemProperty`     | Incoming  |

`InventoryResource` is simultaneously a consumer of two Kafka topics and a producer to one, making it the central hub of this guide's bidirectional flow.

</details>

---

**Q7. What is the `PropertyMessage` model and why is it needed?**

<details>
<summary>Answer</summary>

`PropertyMessage` is a data model with three fields:

```java
public class PropertyMessage {
    public String hostname;  // which system reported the value
    public String key;       // the property name that was requested
    public String value;     // the resolved System.getProperty() value
}
```

It is needed because the response from the System Microservice must carry more than just the value — the Inventory Microservice needs to know which host answered and which property key the value corresponds to, so it can store the result correctly in `InventoryManager`. It follows the same pattern as `SystemLoad`: serializer and deserializer are inner classes backed by JSONB.

</details>

---

**Q8. What is the difference between the channel name in `@Incoming`/`@Outgoing` and the Kafka topic name?**

<details>
<summary>Answer</summary>

They are separate. Annotation values are **logical channel names** internal to the application. Physical **Kafka topic names** are declared in `microprofile-config.properties`. For example:

```properties
# Inventory Microservice — Outgoing
mp.messaging.outgoing.requestSystemProperty.topic=request.system.property

# System Microservice — Incoming (same physical topic, different logical name)
mp.messaging.incoming.propertyRequest.topic=request.system.property
```

The logical channel names on the Inventory side (`requestSystemProperty`) and System side (`propertyRequest`) are different — only the physical topic name (`request.system.property`) must match. This allows each service to choose channel names that make sense in its own context.

</details>

---

**Q9. How does the integration test verify the `PUT /inventory/data` flow without running the System Microservice?**

<details>
<summary>Answer</summary>

The test calls `PUT /inventory/data` via the REST client and then directly polls the `request.system.property` Kafka topic using a `KafkaConsumer`:

```java
Response response = client.updateSystemProperty("os.name");
assertEquals(200, response.getStatus(), "Response should be 200");
ConsumerRecords<String, String> records =
    propertyConsumer.poll(Duration.ofMillis(4000));
assertTrue(records.count() > 0, "No records polled");
for (ConsumerRecord<String, String> record : records) {
    assertEquals("os.name", record.value());
}
```

This verifies that the Inventory Microservice correctly emitted the property name to Kafka — the boundary the Inventory Microservice owns — without needing the System Microservice or the `add.system.property` response topic.

</details>

---

**Q10. Why does the System Microservice consumer group for `propertyRequest` use `group.id=property-name`?**

<details>
<summary>Answer</summary>

```properties
mp.messaging.incoming.propertyRequest.group.id=property-name
```

If multiple System Microservice instances are running, a shared `group.id` ensures Kafka distributes partitions among them — each property request message is processed by exactly one instance rather than duplicated across all instances. Without a `group.id`, Kafka assigns a random unique group to each consumer, which would cause every instance to receive and process every message independently.

</details>

---

**Q11. What would happen if `propertyNameEmitter` were `null` when `PUT /inventory/data` is called?**

<details>
<summary>Answer</summary>

`propertyNameEmitter` is assigned inside the lambda passed to `Flowable.create()`, which the MicroProfile Reactive Messaging runtime executes when it first subscribes to the `Publisher` returned by `sendPropertyName()`. Subscription happens at application startup.

If `PUT /inventory/data` were somehow called before the runtime subscribed (before the application was fully started), calling `propertyNameEmitter.onNext()` would throw a `NullPointerException`. In practice, the Liberty readiness health check (`/health/ready`) prevents traffic from reaching the endpoint until the application is fully initialized and the runtime has subscribed, so the emitter is populated by the time any REST call arrives.

</details>
