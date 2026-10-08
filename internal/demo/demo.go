// Package demo wires the example service's dependencies for the cmd/ binaries.
package demo

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"

	"github.com/temporalio/doordash-sandbox-poc/pkg/shim/shimkafka"
	"github.com/temporalio/doordash-sandbox-poc/service/inventory"
	"github.com/temporalio/doordash-sandbox-poc/service/order"
)

func Env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Activities builds order activities: gRPC to INVENTORY_ADDR and Kafka to
// KAFKA_BROKERS (or an in-process broker with a logging consumer when unset).
// consumerGroup, if set, also starts a real Kafka consumer that logs the
// baggage on every record.
func Activities(ctx context.Context, consumerGroup string) (*order.Activities, func(), error) {
	inv, err := inventory.Dial(Env("INVENTORY_ADDR", "127.0.0.1:50051"))
	if err != nil {
		return nil, nil, err
	}
	var producer shimkafka.Producer
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		bs := strings.Split(brokers, ",")
		producer = shimkafka.NewKafkaProducer(bs...)
		if consumerGroup != "" {
			go consumeKafka(ctx, bs, consumerGroup)
		}
	} else {
		mem := shimkafka.NewMemoryBroker()
		go logConsumer(ctx, mem.Subscribe(order.EventsTopic))
		producer = mem
	}
	return &order.Activities{Inventory: inv, Events: producer}, func() {
		_ = producer.Close()
		_ = inv.Close()
	}, nil
}

func logConsumer(ctx context.Context, ch <-chan kafka.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			h, ev := order.ConsumerHop(msg)
			slog.Info("kafka consumer received", "topic", msg.Topic, "order", ev.OrderID, "status", ev.Status, "baggage", h.Routing.String())
		}
	}
}

func consumeKafka(ctx context.Context, brokers []string, group string) {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, GroupID: group, Topic: order.EventsTopic})
	defer r.Close()
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("kafka read", "err", err)
			}
			return
		}
		h, ev := order.ConsumerHop(msg)
		slog.Info("kafka consumer received", "topic", msg.Topic, "order", ev.OrderID, "status", ev.Status, "baggage", h.Routing.String())
	}
}
