package SheXiang_mq

import (
	uuid "github.com/satori/go.uuid"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var lock = &sync.Mutex{}

var factoryInstance *mqFactory

type (
	mqFactory struct {
		TopicPublishInfoTable map[string] /*TopicName*/ *TopicPublishInfo
		producerTable         map[string] /*ProducerGroup*/ Producer
		consumerTable         map[string] /*ConsumerGroup*/ Consumer
		monitorListener       MonitorListener
	}

	// ToPicConfig Topic 配置
	ToPicConfig struct {
		//名称
		TopicName string
		//队列长度
		MessageQueueLength int
		//队列缓存容量
		MessageCapLength int
		//对应可用线程池大小
		PoolSize int
	}

	FactoryConfig struct {
		TopicConfigTable map[string] /*TopicName*/ *ToPicConfig
	}

	TopicPublishInfo struct {
		ToPicConfig   *ToPicConfig
		oneClose      sync.Once
		messageQueues []MessageQueue
	}
)

func instance(toPicConfigs []*ToPicConfig) *mqFactory {
	if factoryInstance == nil {
		lock.Lock()
		defer lock.Unlock()
		if factoryInstance == nil {
			factoryInstance = &mqFactory{
				producerTable:         make(map[string] /*ProducerGroup*/ Producer),
				consumerTable:         make(map[string] /*ConsumerGroup*/ Consumer),
				TopicPublishInfoTable: createTopicPublishInfoTable(toPicConfigs),
				monitorListener: &Monitor{
					ctx: make(chan int),
				},
			}
		}
	}
	return factoryInstance
}

// 初始化 TopicPublishInfoTable 对应队列不初始化,采用延迟初始化策略
func createTopicPublishInfoTable(toPicConfigs []*ToPicConfig) map[string] /*TopicName*/ *TopicPublishInfo {
	m := make(map[string] /*TopicName*/ *TopicPublishInfo, len(toPicConfigs))
	for _, topicConfig := range toPicConfigs {
		m[topicConfig.TopicName] = &TopicPublishInfo{
			ToPicConfig: topicConfig,
		}
	}
	return m
}

// GetProducer 获取一个topic 对应的生产者
// 采用延迟初始化
func (m *mqFactory) GetProducer(groupName string) Producer {
	return m.GetProducerByConfig(&ProducerConfig{
		groupName,
	})
}

func (m *mqFactory) GetProducerByConfig(config *ProducerConfig) Producer {
	producer := m.producerTable[config.ProducerGroupName]
	//为空 说明没有初始化
	if producer == nil {
		//初始化
		p := newProducer(config.ProducerGroupName, m)
		//注册到mq工厂实例中
		m.registerProducer(config.ProducerGroupName, p)
		//
		producer = m.producerTable[config.ProducerGroupName]
	}
	return producer
}

// GetConsumer 获取一个topic 对应的消费者
// 采用延迟初始化
func (m *mqFactory) GetConsumer(groupName string) Consumer {
	return m.GetConsumerByConfig(&ConsumerConfig{
		ConsumerGroup: groupName,
		PoolSize:      5000,
	})
}

func (m *mqFactory) GetConsumerByConfig(config *ConsumerConfig) Consumer {
	consumer := m.consumerTable[config.ConsumerGroup]
	//为空 说明没有初始化
	if consumer == nil {
		//初始化
		c := newConsumer(config, m)
		//注册到mq工厂实例中
		m.registerConsumer(config.ConsumerGroup, c)
		return c
	}
	return consumer
}

func (m *mqFactory) registerProducer(group string, producer Producer) bool {
	if producer == nil || group == "" {
		return false
	}
	m.producerTable[group] = producer
	return true
}

func (m *mqFactory) unregisterProducer(group string) {
	producer := m.producerTable[group]
	if producer != nil {
		delete(m.producerTable, group)
	}
}
func (m *mqFactory) tryToFindTopicPublishInfo(topic string) *TopicPublishInfo {
	info := m.TopicPublishInfoTable[topic]
	if info == nil {
		return nil
	}

	if info != nil && info.messageQueues != nil {
		return info
	}
	//初始化队列
	info.messageQueues = m.createMessageQueues(info.ToPicConfig)
	return info
}

func (m *mqFactory) selectOneMessageQueue(info *TopicPublishInfo) *MessageQueue {
	return info.selectOneMessageQueue()
}

func (m *mqFactory) createMessageQueues(config *ToPicConfig) []MessageQueue {
	q := make([]MessageQueue, 0)
	for i := 0; i < config.MessageQueueLength; i++ {
		queue := MessageQueue{
			Message: make(chan Message, config.MessageCapLength),
			QueueId: uuid.NewV4().String(),
		}
		q = append(q, queue)
	}
	return q
}

func (m *mqFactory) registerConsumer(group string, consumer Consumer) bool {
	if consumer == nil || group == "" {
		return false
	}
	m.consumerTable[group] = consumer
	return true
}

func (m *mqFactory) unregisterConsumer(group string) {
	consumer := m.consumerTable[group]
	if consumer != nil {
		delete(m.consumerTable, group)
	}
}

func (topic *TopicPublishInfo) TopicBlockageMessageQueueCount() int64 {
	var i int64 = 0
	for _, queue := range topic.messageQueues {
		atomic.AddInt64(&i, int64(len(queue.Message)))
	}
	return atomic.LoadInt64(&i)
}

func (topic *TopicPublishInfo) selectOneMessageQueue() *MessageQueue {
	rand.NewSource(time.Now().UnixNano())
	return &topic.messageQueues[rand.Intn(len(topic.messageQueues))]
}
