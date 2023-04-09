package SheXiang_mq

import (
	"fmt"
	"github.com/panjf2000/ants/v2"
	"sync"
	"sync/atomic"
)

type (
	Message struct {
		Topic      string
		Body       []byte
		Properties map[string]any
	}

	MessageQueue struct {
		QueueId string
		Message chan Message
	}

	MessageQueueSelector interface {
		Select(messageQueues []*MessageQueue) *MessageQueue
	}

	// ToPicConfig Topic 配置
	ToPicConfig struct {
		//名称
		TopicName string
		//队列长度
		MessageQueueLength int
		//队列缓存容量
		MessageCapLength int
		//Topic 队列选择器 //默认随机
		Selector MsgQueueSelector
		// 可用线程数
		PoolSize int
		//pool
		pool *ants.Pool
	}

	TopicPublishInfo struct {
		ToPicConfig          *ToPicConfig
		MessageQueueSelector MessageQueueSelector
		oneClose             sync.Once
		messageQueues        []*MessageQueue
	}
)

func (topic *TopicPublishInfo) TopicBlockageMessageQueueCount() int64 {
	var i int64 = 0
	for _, queue := range topic.messageQueues {
		atomic.AddInt64(&i, int64(len(queue.Message)))
	}
	return atomic.LoadInt64(&i)
}

func (topic *TopicPublishInfo) selectOneMessageQueue() *MessageQueue {
	return topic.MessageQueueSelector.Select(topic.messageQueues)
}
func (topic *TopicPublishInfo) removeTopic() {
	topic.oneClose.Do(func() {
		for _, queue := range topic.messageQueues {
			close(queue.Message)
		}
	})
}

func NewToPicConfig(topicName string) *ToPicConfig {
	topic := &ToPicConfig{
		TopicName:          topicName,
		MessageQueueLength: DefaultMessageQueueLength,
		MessageCapLength:   DefaultMessageCapLength,
		PoolSize:           DefaultPoolSize,
	}
	topic.pool = newToPicPool(topic)
	return topic
}
func NewToPicConfigByPoolSize(topicName string, DefaultTopicPoolSize int) *ToPicConfig {
	topic := &ToPicConfig{
		TopicName:          topicName,
		MessageQueueLength: DefaultMessageQueueLength,
		MessageCapLength:   DefaultMessageCapLength,
		PoolSize:           DefaultTopicPoolSize,
	}
	topic.pool = newToPicPool(topic)
	return topic
}

func (topic *ToPicConfig) getMessageQueueLength() int {
	if topic.MessageQueueLength <= 0 {
		return DefaultMessageQueueLength
	}
	return topic.MessageQueueLength
}
func (topic *ToPicConfig) getMessageCapLength() int {
	if topic.MessageCapLength <= 0 {
		return DefaultMessageCapLength
	}
	return topic.MessageCapLength
}

func (topic *ToPicConfig) getPoolSize() int {
	if topic.PoolSize <= 0 {
		return DefaultPoolSize
	}
	return topic.PoolSize
}

func newToPicPool(topic *ToPicConfig) *ants.Pool {
	//fmt.Printf("newPool topic %v \n", topic.TopicName)
	poolSize := topic.getPoolSize() + topic.getMessageQueueLength()*topic.getMessageCapLength()
	pool, err := ants.NewPool(poolSize)
	if err != nil {
		fmt.Printf("init topic [%v] config Pool fail \n", topic.TopicName)
		return nil
	}
	return pool
}
