package kafka_client

import (
	"errors"
	"fmt"
	"github.com/json-iterator/go"
)

/**
压缩算法:

CompressionNone    = 0
CompressionGZIP    = 1
CompressionSnappy  = 2
CompressionLZ4     = 3
CompressionZSTD    = 4
*/
type KafkaProperties struct {
	BootstrapServers []string `json:"bootstrap_servers"`
	GroupId          string   `json:"group_id"`
	AutoOffsetReset  string   `json:"auto_offset_reset"`
	Topic            []string `json:"topic"`
	Compression      int      `json:"compression"`
	Version          string   `json:"version"`
}

func (kp KafkaProperties) String() string {
	if v, e := jsoniter.MarshalToString(kp); e != nil {
		return fmt.Sprintf("[KafkaProperties] toJSONString error: %v", e)
	} else {
		return v
	}
}

func (kp *KafkaProperties) ValidateProducer() error {
	if len(kp.BootstrapServers) == 0 {
		return errors.New("producer's bootstrap.servers is not set")
	}
	if len(kp.Topic) == 0 {
		return errors.New("producer's topic is not set")
	}
	return nil
}

func (kp *KafkaProperties) ValidateConsumer() error {
	if len(kp.BootstrapServers) == 0 {
		return errors.New("consumer's bootstrap.servers is not set")
	}
	if len(kp.Topic) == 0 {
		return errors.New("consumer's topic is not set")
	}
	if len(kp.GroupId) == 0 {
		return errors.New("consumer's group.id is not set")
	}
	if len(kp.AutoOffsetReset) == 0 {
		return errors.New("consumer's auto.offset.reset is not set")
	}
	return nil
}
