package SheXiang_mq

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type (
	SelectMessageQueueByRandom  struct{}
	SelectMessageQueueByPolling struct {
		//轮询点位
		pollingPoint *int32
	}
)

// Select 随机选择消息队列
func (s *SelectMessageQueueByRandom) Select(messageQueues []*MessageQueue) *MessageQueue {
	rand.NewSource(time.Now().UnixNano())
	return messageQueues[rand.Intn(len(messageQueues))]
}

// Select 通过轮询选择消息队列
func (s *SelectMessageQueueByPolling) Select(messageQueues []*MessageQueue) *MessageQueue {
	point := atomic.LoadInt32(s.pollingPoint)
	queue := messageQueues[point]
	atomic.AddInt32(&point, 1)
	if point > int32(len(messageQueues)-1) {
		atomic.CompareAndSwapInt32(&point, point, 0)
	}
	return queue
}

func MessageQueueSelectorCreate(selector MsgQueueSelector) MessageQueueSelector {
	switch selector {
	case Random:
		return &SelectMessageQueueByRandom{}
	case Polling:
		var polling int32 = 0
		return &SelectMessageQueueByPolling{pollingPoint: &polling}
	default:
		return &SelectMessageQueueByRandom{}
	}
}
