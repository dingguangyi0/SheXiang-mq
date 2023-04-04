package SheXiang_mq

import (
	"errors"
	"fmt"
)

var (
	// ErrShutdownAlready the producer service state not OK, maybe started once
	ErrShutdownAlready = errors.New("the producer service state not OK, maybe started once")
)

type MqConfig struct {
	ProducerConfig *ProducerConfig
	ConsumerConfig *ConsumerConfig
	ToPicConfigs   []*ToPicConfig
}

type Mq struct {
	Consumer        Consumer
	Producer        Producer
	MonitorListener MonitorListener
}

func (mq *MqConfig) New() *Mq {
	//check if we have a consumer already and create
	checkConfig(mq)
	//初始化Mq工厂
	mqFactory := instance(mq.ToPicConfigs)
	//创建消费者
	consumer := mqFactory.GetConsumerByConfig(mq.ConsumerConfig)
	//注册Topic监听
	for topicName, listener := range mq.ConsumerConfig.MessageListeners {
		consumer.Subscribe(topicName)
		consumer.RegisterMessageListener(topicName, listener)
	}
	//启动配置类
	err := consumer.Start()
	if err != nil {
		fmt.Println("Consumer Start()", err)
		return nil
	}
	producer := mqFactory.GetProducerByConfig(mq.ProducerConfig)
	//启动消费者类
	err = producer.Start()
	if err != nil {
		fmt.Println("Producer Start()", err)
		return nil
	}
	return &Mq{
		Consumer:        consumer,
		Producer:        producer,
		MonitorListener: mqFactory.monitorListener,
	}
}

func checkConfig(mq *MqConfig) {

}
