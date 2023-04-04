package SheXiang_mq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// 生产者 发布消息数
// topic 消费者 消费数 消费时间最大时间 最小时间 平均值 总时间
// 1

type (
	Monitor struct {
		msgs       sync.Map
		ctx        chan int
		topicInfos []*TopicPublishInfo
	}

	Msg struct {
		// topicName name for monitoring
		topicName string
		// consumerGroup name for monitoring
		consumerGroup string
		// taskCount task count for monitoring
		taskCount *int64
		// timeCount time count for monitoring
		timeCount *int64
		// maxTime max time task  for monitoring
		maxTime *int64
		// minTime min time task for monitoring
		minTime *int64
		// started
		startTime time.Time
		//
		lastTaskTime *int64

		toPicConfig *TopicPublishInfo
	}
)

func (monitor *Monitor) Init(topicInfos []*TopicPublishInfo) {
	for _, topicInfo := range topicInfos {
		monitor.msgs.Store(topicInfo.ToPicConfig.TopicName, Msg{})
	}
}
func (monitor *Monitor) InitByTopic(topicInfo *TopicPublishInfo) {
	var timeCount int64 = 0
	var taskCount int64 = 0
	var maxTime int64 = 0
	var minTime int64 = 0
	var lastTaskTime int64 = 0
	monitor.msgs.Store(topicInfo.ToPicConfig.TopicName, Msg{
		topicName:    topicInfo.ToPicConfig.TopicName,
		taskCount:    &taskCount,
		timeCount:    &timeCount,
		maxTime:      &maxTime,
		minTime:      &minTime,
		startTime:    time.Now(),
		lastTaskTime: &lastTaskTime,
		toPicConfig:  topicInfo,
	})
}

func (monitor *Monitor) surround(messageListener func(message Message) ConsumeConcurrentlyStatus, msg Message) {
	now := time.Now()
	messageListener(msg)
	monitor.add(msg.Topic, now)
}

func (monitor *Monitor) add(topic string, now time.Time) {
	since := time.Since(now)
	v, ok := monitor.msgs.Load(topic)
	if ok {
		msg := v.(Msg)
		atomic.AddInt64(msg.taskCount, 1)
		atomic.AddInt64(msg.timeCount, int64(since))
		maxT := atomic.LoadInt64(msg.maxTime)
		minT := atomic.LoadInt64(msg.minTime)
		atomic.CompareAndSwapInt64(msg.maxTime, maxT, max(maxT, int64(since)))
		atomic.CompareAndSwapInt64(msg.minTime, minT, max(minT, int64(since)))
		atomic.CompareAndSwapInt64(msg.lastTaskTime, atomic.LoadInt64(msg.lastTaskTime), int64(time.Since(msg.startTime)))
	}
}

func (monitor *Monitor) Info() {
	monitor.msgs.Range(func(key, value any) bool {
		msg := value.(Msg)
		fmt.Printf("Topic: %v,容量: %v monitor: %v", key, msg.toPicConfig.TopicBlockageMessageQueueCount(), msg.info())
		return true
	})
}

func (monitor *Monitor) TurnMonitor() {
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		for {
			select {
			case <-ticker.C:
				monitor.Info()
			case <-monitor.ctx:
				fmt.Println("Monitor Turned closed")
			}
		}
	}()
}

func (monitor *Monitor) CloseMonitor() {
	close(monitor.ctx)
}

func (m Msg) info() string {
	var timeCount int64 = *m.timeCount
	var taskCount int64 = *m.taskCount
	if taskCount == 0 {
		taskCount = 1
	}
	return fmt.Sprintf("执行总数: %v 累加执行时间: %v,队列实际执行时间: %v 执行最小时间:%v 执行最大时间：%v 执行平均时间：%v \n", *m.taskCount, time.Duration(*m.timeCount), time.Duration(*m.lastTaskTime), time.Duration(*m.minTime), time.Duration(*m.maxTime), time.Duration(timeCount/taskCount))
}

func max(o, n int64) int64 {
	if o > n {
		return o

	}
	return n
}

func min(o, n int64) int64 {
	if o > n {
		return n

	}
	return o
}
