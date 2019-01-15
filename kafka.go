package kafka_client

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

type KafkaClient struct {
	conf     KafkaProperties
	consumer *cluster.Consumer
	producer sarama.AsyncProducer
	version  sarama.KafkaVersion
	receiver func(message *sarama.ConsumerMessage)
}

func NewKafkaClient(conf KafkaProperties, receiver func(message *sarama.ConsumerMessage)) *KafkaClient {
	if receiver == nil {
		log.Fatalf("kafka client message receiver not present")
	}
	client := KafkaClient{conf: conf, receiver: receiver}
	if v, err := sarama.ParseKafkaVersion(conf.Version); err != nil {
		log.Fatalf("%v", err)
	} else {
		client.version = v
	}
	client.newConsumer()
	client.newProducer()
	return &client
}

func (kc *KafkaClient) SendMessage(message *sarama.ProducerMessage) error {
	if kc.producer == nil {
		return errors.New("producer is not prepared.")
	}
	kc.producer.Input() <- message
	return nil
}

func (kc *KafkaClient) CommitOffsets() error {
	if kc.consumer == nil {
		return errors.New("consumer is not prepared.")
	}
	return kc.consumer.Close()
}

func (kc *KafkaClient) Destroy() {
	if e := kc.CommitOffsets(); e != nil {
		log.Errorf("commit offset error : %v", e)
	}
	if kc.consumer != nil {
		if e := kc.consumer.Close(); e != nil {
			log.Errorf("close kafka consumer error : %v", e)
		}
	}
	if kc.producer != nil {
		if e := kc.producer.Close(); e != nil {
			log.Errorf("close kafka producer error : %v", e)
		}
	}
}

func (kc *KafkaClient) GetConf() KafkaProperties {
	return kc.conf
}

func (kc *KafkaClient) newConsumer() {
	//
	if err := kc.conf.ValidateConsumer(); err != nil {
		log.Fatalf("%v", err)
	}
	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Version = kc.version
	config.Consumer.Return.Errors = true
	if strings.EqualFold(kc.conf.AutoOffsetReset, "latest") {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest // 最近的
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	config.Group.Return.Notifications = true
	//
	consumer, err := cluster.NewConsumer(kc.conf.BootstrapServers, kc.conf.GroupId, kc.conf.Topic, config)
	if err != nil {
		log.Fatalf("create Kafka consumer failed : %v", err)
	}
	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Errorf("Kafka Consumer Error: %s\n", err.Error())
		}
	}()
	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Warnf("Kafka Consumer Rebalanced: %+v\n", ntf)
		}
	}()
	// receiver
	go func() {
		for {
			select {
			case msg, ok := <-consumer.Messages():
				if ok {
					kc.receiver(msg)
					consumer.MarkOffset(msg, "") // mark message as processed
				}
			}
		}
	}()
	//
	kc.consumer = consumer
}

func (kc *KafkaClient) newProducer() {
	//
	if err := kc.conf.ValidateProducer(); err != nil {
		log.Fatalf("%v", err)
	}
	// For the access log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	config := sarama.NewConfig()
	config.Version = kc.version
	config.Producer.RequiredAcks = sarama.WaitForAll                           // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionCodec(kc.conf.Compression) // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond                   // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(kc.conf.BootstrapServers, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}
	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Errorf("Failed to write kafka log entry: %v", err)
		}
	}()
	kc.producer = producer
}
