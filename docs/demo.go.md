## `internal/demo/demo.go`

This file builds the outbound dependencies that the order activities need, so the `cmd/` binaries can run them. It is demo wiring only; the shim library and the order service don't depend on it.

### `Env(k, def)`
Returns environment variable `k`, or `def` if it's unset. `cmd/inventory` uses it too, to choose its listen address.

### `Activities(ctx, consumerGroup)`
This is the entry point. It returns an `*order.Activities`, a cleanup function and an error.

1. **gRPC:** it connects to the inventory service at `INVENTORY_ADDR`, defaulting to `127.0.0.1:50051`. The client from `inventory.Dial` already includes the `shimgrpc` interceptor, so every call puts the activity's baggage (`ptid`, `sbr`, `rid`) into the gRPC metadata.
2. **Kafka:** it picks one of two modes.
   - **`KAFKA_BROKERS` is set:** it creates a real `shimkafka.KafkaProducer`. If `consumerGroup` is non-empty, it also starts `consumeKafka` in a goroutine.
   - **`KAFKA_BROKERS` is unset (the default):** it creates an in-process `MemoryBroker`, subscribes to `order-events`, and starts `logConsumer` in a goroutine.

   Either producer puts the baggage into the record headers when it publishes.
3. **Cleanup:** the returned function closes the producer and the inventory connection. Callers `defer` it.

### `logConsumer` and `consumeKafka`
Both read `order-events` records and pass each one to `order.ConsumerHop`. That function pulls the baggage back out of the headers and decodes the event. Each record is then logged as:

```
kafka consumer received topic=order-events order=1001 status=reserved baggage="ptid=dashtest sbr=… rid=…"
```

This log line is how the demo shows that baggage survives the trip through Kafka.
- `logConsumer` reads from the in-memory channel until `ctx` is cancelled.
- `consumeKafka` reads from a real consumer group. It stops on the first read error and logs a warning only if the error wasn't caused by shutdown. It doesn't retry.

### Who calls it
- **`cmd/prodworker`** calls `Activities(ctx, "order-events-audit")`. With real Kafka, it's the process that consumes the topic.
- **`cmd/devtest`** calls `Activities(ctx, "")`. It produces events but never starts a real-Kafka consumer.

### Behaviours worth knowing
- **In-memory mode is per process.** Each binary gets its own broker, so a developer run's events are logged in that run's own log (`.demo-logs/alice.log`), not in `prodworker.log`. That's why the demo's final "what the Kafka consumer saw" section lists only production and carol traffic.
- **Publishing can block in in-memory mode.** Each subscriber channel holds 1,024 messages. If `ctx` is cancelled, `logConsumer` stops reading, and later publishes wait until the activity's own context ends. Only a long run after shutdown has started would hit this.