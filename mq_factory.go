package SheXiang_mq

import (
	uuid "github.com/satori/go.uuid"
	"sync"
)

var lock = &sync.Mutex{}

var factoryInstance *MqFactory

type (
	MqFactory struct {
		TopicPublishInfoTable map[string] /*TopicName*/ *TopicPublishInfo
		producerTable         map[string] /*ProducerGroup*/ Producer
		consumerTable         map[string] /*ConsumerGroup*/ Consumer
		monitorListener       MonitorListener
	}

	FactoryConfig struct {
		ToPicConfigs []*ToPicConfig
		//Topic 队列选择器 选填默认随机 优先ToPicConfigs 中
		Selector MsgQueueSelector
	}
)

func Instance(config *FactoryConfig) *MqFactory {
	if factoryInstance == nil {
		lock.Lock()
		defer lock.Unlock()
		if factoryInstance == nil {
			factoryInstance = &MqFactory{
				producerTable:         make(map[string] /*ProducerGroup*/ Producer),
				consumerTable:         make(map[string] /*ConsumerGroup*/ Consumer),
				TopicPublishInfoTable: createTopicPublishInfoTable(config.ToPicConfigs),
			}
			factoryInstance.monitorListener = NewMonitor(factoryInstance)
		}
	}
	return factoryInstance
}

// 初始化 TopicPublishInfoTable 对应队列不初始化,采用延迟初始化策略
func createTopicPublishInfoTable(toPicConfigs []*ToPicConfig) map[string] /*TopicName*/ *TopicPublishInfo {
	m := make(map[string] /*TopicName*/ *TopicPublishInfo, len(toPicConfigs))
	for _, topicConfig := range toPicConfigs {
		m[topicConfig.TopicName] = &TopicPublishInfo{
			ToPicConfig:          topicConfig,
			MessageQueueSelector: MessageQueueSelectorCreate(topicConfig.Selector),
		}
	}
	return m
}

// GetProducer 获取一个topic 对应的生产者
// 采用延迟初始化
func (m *MqFactory) GetProducer() Producer {
	return m.GetProducerByGroup(DefaultProducerGroup)
}

// GetProducer 获取一个topic 对应的生产者
// 采用延迟初始化
func (m *MqFactory) GetProducerByGroup(groupName string) Producer {
	return m.GetProducerByConfig(&ProducerConfig{
		groupName,
	})
}

func (m *MqFactory) GetProducerByConfig(config *ProducerConfig) Producer {
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
func (m *MqFactory) GetConsumer() Consumer {
	return m.GetConsumerByGroup(DefaultConsumerGroup)
}

// GetConsumer 获取一个topic 对应的消费者
// 采用延迟初始化
func (m *MqFactory) GetConsumerByGroup(groupName string) Consumer {
	return m.GetConsumerByConfig(&ConsumerConfig{
		ConsumerGroup: groupName,
	})
}

func (m *MqFactory) GetConsumerByConfig(config *ConsumerConfig) Consumer {
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

func (m *MqFactory) registerProducer(group string, producer Producer) bool {
	if producer == nil || group == "" {
		return false
	}
	m.producerTable[group] = producer
	return true
}

func (m *MqFactory) unregisterProducer(group string) {
	producer := m.producerTable[group]
	if producer != nil {
		delete(m.producerTable, group)
	}
}
func (m *MqFactory) tryToFindTopicPublishInfo(topic string) *TopicPublishInfo {
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

func (m *MqFactory) selectOneMessageQueue(info *TopicPublishInfo) *MessageQueue {
	return info.selectOneMessageQueue()
}

func (m *MqFactory) createMessageQueues(config *ToPicConfig) []*MessageQueue {
	q := make([]*MessageQueue, 0)
	for i := 0; i < config.MessageQueueLength; i++ {
		queue := &MessageQueue{
			Message: make(chan Message, config.getMessageCapLength()),
			QueueId: uuid.NewV4().String(),
		}
		q = append(q, queue)
	}
	return q
}

func (m *MqFactory) registerConsumer(group string, consumer Consumer) bool {
	if consumer == nil || group == "" {
		return false
	}
	m.consumerTable[group] = consumer
	return true
}

func (m *MqFactory) unregisterConsumer(group string) {
	consumer := m.consumerTable[group]
	if consumer != nil {
		delete(m.consumerTable, group)
	}
}
