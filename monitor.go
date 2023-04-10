package SheXiang_mq

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// 生产者 发布消息数
// topic 消费者 消费数 消费时间最大时间 最小时间 平均值 总时间
// 1

type (
	Monitor struct {
		msgs       map[string] /*TopicName*/ *Msg
		ctx        chan int
		topicInfos []*TopicPublishInfo
		mqFactory  *MqFactory
	}

	Msg struct {
		// topicName name for monitoring
		TopicName string
		// consumerGroup name for monitoring
		ConsumerGroup string
		// taskCount task count for monitoring
		TaskCount int64
		// timeCount time count for monitoring
		TimeCount int64
		// maxTime max time task  for monitoring
		MaxTime int64
		// minTime min time task for monitoring
		MinTime int64

		Running int64

		Waiting int64
		// started
		StartTime time.Time
		//
		LastTaskTime int64
		//
		toPicConfig *TopicPublishInfo
	}
)

func NewMonitor(mqFactory *MqFactory) *Monitor {
	m := make(map[string]*Msg)
	now := time.Now()
	for key, info := range mqFactory.TopicPublishInfoTable {
		m[key] = &Msg{
			MinTime:     -1,
			TopicName:   key,
			StartTime:   now,
			toPicConfig: info,
		}
	}
	return &Monitor{
		mqFactory: mqFactory,
		msgs:      m,
		ctx:       make(chan int),
	}
}

func (m *Monitor) Surround(listener func(message Message) ConsumeConcurrentlyStatus, msg Message) {
	now := time.Now()
	listener(msg)
	m.add(msg.Topic, now)
}

func (m *Monitor) add(topic string, now time.Time) {
	since := time.Since(now)
	pool := m.msgs[topic].toPicConfig.ToPicConfig.pool
	atomic.AddInt64(&m.msgs[topic].TaskCount, 1)
	atomic.AddInt64(&m.msgs[topic].TimeCount, int64(since))

	atomic.CompareAndSwapInt64(&m.msgs[topic].MaxTime, m.msgs[topic].MaxTime, max(m.msgs[topic].MaxTime, int64(since)))
	atomic.CompareAndSwapInt64(&m.msgs[topic].MinTime, m.msgs[topic].MinTime, min(m.msgs[topic].MinTime, int64(since)))
	atomic.CompareAndSwapInt64(&m.msgs[topic].LastTaskTime, m.msgs[topic].LastTaskTime, int64(time.Since(m.msgs[topic].StartTime)))
	atomic.CompareAndSwapInt64(&m.msgs[topic].Running, m.msgs[topic].Running, int64(pool.Running()))
	atomic.CompareAndSwapInt64(&m.msgs[topic].Waiting, m.msgs[topic].Running, int64(pool.Waiting()))
}

func (m *Monitor) Print() {
	keys := make([]string, 0)
	for key, _ := range m.msgs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		msg := m.msgs[key]
		sprintf := fmt.Sprintf("Topic: %v,容量: %v monitor: %v", key, msg.toPicConfig.TopicBlockageMessageQueueCount(), msg.info())
		fmt.Println(sprintf)
	}
	fmt.Println("================================================================")
}

func (m *Monitor) Info() map[string]*Msg {
	return m.msgs
}

func (m *Monitor) TurnMonitor() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				m.Print()
			case <-m.ctx:
				fmt.Println("Monitor Turned closed")
				return
			}
		}
	}()
}

func (m *Monitor) CloseMonitor() {
	close(m.ctx)
}

func (m *Msg) info() string {
	if m.TaskCount == 0 {
		m.TaskCount = 1
	}
	pool := m.toPicConfig.ToPicConfig.pool
	return fmt.Sprintf("执行总数:[%v] 运行任务数:[%v] 阻塞任务数:[%v] 累加执行时间:[%v],队列实际执行时间:[%v] 执行最小时间:[%v] 执行最大时间:[%v] 执行平均时间:[%v] ", m.TaskCount, pool.Running(), pool.Waiting(), time.Duration(m.TimeCount), time.Duration(m.LastTaskTime), time.Duration(m.MinTime), time.Duration(m.MaxTime), time.Duration(m.TimeCount/m.TaskCount))
}

func max(o, n int64) int64 {
	if o > n {
		return o

	}
	return n
}

func min(o, n int64) int64 {
	if o > n || o == -1 {
		return n
	}
	return o
}

func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}
