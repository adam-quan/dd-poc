// Package shimkafka carries the shim's OTel baggage (ptid/sbr/rid) and trace
// context in Kafka record headers.
package shimkafka

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"

	"github.com/temporalio/doordash-sandbox-poc/pkg/shim"
)

// headerCarrier adapts Kafka headers to an OTel TextMapCarrier.
type headerCarrier struct{ h *[]kafka.Header }

func (c headerCarrier) Get(k string) string {
	for _, h := range *c.h {
		if h.Key == k {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(k, v string) {
	for i, h := range *c.h {
		if h.Key == k {
			(*c.h)[i].Value = []byte(v)
			return
		}
	}
	*c.h = append(*c.h, kafka.Header{Key: k, Value: []byte(v)})
}

func (c headerCarrier) Keys() []string {
	out := make([]string, 0, len(*c.h))
	for _, h := range *c.h {
		out = append(out, h.Key)
	}
	return out
}

// Inject writes the ctx baggage into msg's headers.
func Inject(ctx context.Context, msg *kafka.Message) {
	shim.Propagator.Inject(ctx, headerCarrier{&msg.Headers})
}

// Extract returns ctx enriched with the baggage found in msg's headers.
func Extract(ctx context.Context, msg kafka.Message) context.Context {
	h := msg.Headers
	return shim.Propagator.Extract(ctx, headerCarrier{&h})
}

// Producer publishes records, propagating baggage from ctx.
type Producer interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
	Close() error
}

// KafkaProducer publishes to a real cluster.
type KafkaProducer struct{ w *kafka.Writer }

func NewKafkaProducer(brokers ...string) *KafkaProducer {
	return &KafkaProducer{w: &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		AllowAutoTopicCreation: true,
		BatchSize:              1,
	}}
}

func (p *KafkaProducer) Publish(ctx context.Context, topic string, key, value []byte) error {
	msg := kafka.Message{Topic: topic, Key: key, Value: value}
	Inject(ctx, &msg)
	return p.w.WriteMessages(ctx, msg)
}

func (p *KafkaProducer) Close() error { return p.w.Close() }

// MemoryBroker is an in-process stand-in for Kafka with the same message and
// header types, used by tests and when no brokers are configured.
type MemoryBroker struct {
	mu   sync.Mutex
	subs map[string][]chan kafka.Message
}

func NewMemoryBroker() *MemoryBroker { return &MemoryBroker{subs: map[string][]chan kafka.Message{}} }

func (b *MemoryBroker) Publish(ctx context.Context, topic string, key, value []byte) error {
	msg := kafka.Message{Topic: topic, Key: key, Value: value}
	Inject(ctx, &msg)
	b.mu.Lock()
	subs := append([]chan kafka.Message(nil), b.subs[topic]...)
	b.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Subscribe returns a channel receiving every record published to topic.
func (b *MemoryBroker) Subscribe(topic string) <-chan kafka.Message {
	ch := make(chan kafka.Message, 1024)
	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], ch)
	b.mu.Unlock()
	return ch
}

func (b *MemoryBroker) Close() error { return nil }
