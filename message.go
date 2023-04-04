package SheXiang_mq

import (
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

func NewToPicConfig(topicName string) *ToPicConfig {
	return &ToPicConfig{
		TopicName:          topicName,
		MessageQueueLength: DefaultMessageQueueLength,
		MessageCapLength:   DefaultMessageCapLength,
	}
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
