package SheXiang_mq

import (
	"errors"
	"fmt"
)

var (
	// ErrShutdownAlready the producer service state not OK, maybe started once
	ErrShutdownAlready = errors.New("the producer service state not OK, maybe started once")
	// ConsumerConfigEmpty Consumer configuration required for MQ is empty
	ConsumerConfigEmpty = errors.New("consumer configuration required for MQ is empty")
	//ConsumerConfigMessageListenersEmpty consumer configuration MessageListeners required for MQ is empty
	ConsumerConfigMessageListenersEmpty = errors.New("consumer configuration MessageListeners required for MQ is empty")
)

type MqConfig struct {
	//Topic 队列选择器 //默认随机 ToPicConfig 不配置 默认去mq
	Selector       MsgQueueSelector
	ProducerConfig *ProducerConfig
	ConsumerConfig *ConsumerConfig
	ToPicConfigs   []*ToPicConfig
}

type Mq struct {
	Consumer        Consumer
	Producer        Producer
	MonitorListener MonitorListener
}

func (mq *MqConfig) New() (*Mq, error) {
	//check if we have a consumer already and create
	err := checkConfig(mq)
	if err != nil {
		return nil, err
	}
	//初始化Mq工厂
	mqFactory := Instance(&FactoryConfig{
		ToPicConfigs: mq.ToPicConfigs,
		Selector:     mq.Selector,
	})
	//创建消费者
	consumer := mqFactory.GetConsumerByConfig(mq.ConsumerConfig)
	//注册Topic监听
	for topicName, listener := range mq.ConsumerConfig.MessageListeners {
		consumer.Subscribe(topicName)
		consumer.RegisterMessageListener(topicName, listener)
	}
	//创建生产者
	producer := mqFactory.GetProducerByConfig(mq.ProducerConfig)

	return &Mq{
		Consumer:        consumer,
		Producer:        producer,
		MonitorListener: mqFactory.monitorListener,
	}, nil
}

// New 快捷创建mq 并使用 默认启用 consumer producer
func (mq *MqConfig) NewStart() (*Mq, error) {
	m, err := mq.New()

	err = m.Consumer.Start()
	if err != nil {
		fmt.Println("Consumer Start() err", err)
		return nil, err
	}
	//启动消费者类
	err = m.Producer.Start()
	if err != nil {
		fmt.Println("Producer Start() err", err)
		return nil, err
	}
	return &Mq{
		Consumer:        m.Consumer,
		Producer:        m.Producer,
		MonitorListener: m.MonitorListener,
	}, nil
}

func checkConfig(mq *MqConfig) error {
	if mq.ProducerConfig == nil {
		mq.ProducerConfig = &ProducerConfig{
			DefaultProducerGroup,
		}
	}
	if mq.ConsumerConfig == nil {
		fmt.Println("consumer configuration required for MQ is empty")
		return ConsumerConfigEmpty
	}

	if mq.ConsumerConfig.ConsumerGroup == "" {
		mq.ConsumerConfig.ConsumerGroup = DefaultConsumerGroup
	}

	if mq.ConsumerConfig.MessageListeners == nil {
		fmt.Println("consumer configuration MessageListeners required for MQ is empty")
		return ConsumerConfigMessageListenersEmpty
	}

	if len(mq.ToPicConfigs) == 0 {
		for topic, _ := range mq.ConsumerConfig.MessageListeners {
			mq.ToPicConfigs = append(mq.ToPicConfigs, NewToPicConfig(topic))
		}
	}
	return nil
}
