package queue

import (
	"context"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaProducer implements queue.Producer interface with Kafka as a backing store
type KafkaProducer struct {
	kafka *kgo.Client
	topic string
}

func NewKafkaProducer(brokers []string, topic string) (*KafkaProducer, error) {
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{kafka: kafka, topic: topic}, nil
}

func (p *KafkaProducer) Close() error {
	p.kafka.Close()
	return nil
}

func (p *KafkaProducer) Produce(ctx context.Context, tasks []string) error {
	recs := make([]*kgo.Record, 0, len(tasks))
	for _, t := range tasks {
		recs = append(recs, &kgo.Record{Topic: p.topic, Value: []byte(t)})
	}
	results := p.kafka.ProduceSync(ctx, recs...)
	err := results.FirstErr()
	if err != nil {
		logResultsErrors(results)
		return err
	}
	return nil
}

func logResultsErrors(results kgo.ProduceResults) {
	var errStrings []string
	for _, r := range results {
		if r.Err != nil {
			errStrings = append(errStrings, r.Err.Error())
		}
	}
	log.Error().Msg(strings.Join(errStrings, "; "))
}

// KafkaConsumer implements queue.Consumer interface
type KafkaConsumer struct {
	kafka *kgo.Client
}

func NewKafkaConsumer(brokers []string, topics []string, cgroup string) (*KafkaConsumer, error) {
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup(cgroup),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{kafka: kafka}, nil
}

func (c *KafkaConsumer) Close() error {
	c.kafka.Close()
	return nil
}

func (c *KafkaConsumer) Consume(ctx context.Context, max int) ([]string, error) {
	fetches := c.kafka.PollRecords(ctx, max)
	err := fetches.Err()
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, fetches.NumRecords())

	it := fetches.RecordIter()
	for !it.Done() {
		record := it.Next()
		result = append(result, string(record.Value))
	}

	return result, nil
}

func (c *KafkaConsumer) CommitConsumed(ctx context.Context) error {
	return c.kafka.CommitUncommittedOffsets(ctx)
}
