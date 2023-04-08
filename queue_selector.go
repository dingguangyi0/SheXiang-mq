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
		pollingPoint int32
	}
)

// Select 随机选择消息队列
func (s *SelectMessageQueueByRandom) Select(messageQueues []*MessageQueue) *MessageQueue {
	rand.NewSource(time.Now().UnixNano())
	return messageQueues[rand.Intn(len(messageQueues))]
}

// Select 通过轮询选择消息队列
func (s *SelectMessageQueueByPolling) Select(messageQueues []*MessageQueue) *MessageQueue {
	if atomic.LoadInt32(&s.pollingPoint) > int32(len(messageQueues)-1) {
		atomic.CompareAndSwapInt32(&s.pollingPoint, atomic.LoadInt32(&s.pollingPoint), 0)
	}
	point := atomic.LoadInt32(&s.pollingPoint)
	queue := messageQueues[point]
	atomic.AddInt32(&s.pollingPoint, 1)
	return queue
}

func MessageQueueSelectorCreate(selector MsgQueueSelector) MessageQueueSelector {
	switch selector {
	case Random:
		return &SelectMessageQueueByRandom{}
	case Polling:
		return &SelectMessageQueueByPolling{}
	default:
		return &SelectMessageQueueByRandom{}
	}
}
