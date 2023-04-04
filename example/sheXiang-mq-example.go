package main

import (
	m "SheXiang-mq"
	"math/rand"
	"strconv"
	"time"
)

func main() {
	//init mq config file
	config := &m.MqConfig{
		//Can be omitted
		ProducerConfig: &m.ProducerConfig{
			ProducerGroupName: "ProductName",
		},
		ConsumerConfig: &m.ConsumerConfig{
			PoolSize:      10,
			ConsumerGroup: "ConsumerGroup",
			MessageListeners: map[string]func(message m.Message) m.ConsumeConcurrentlyStatus{
				"topic-test": func(message m.Message) m.ConsumeConcurrentlyStatus {
					duration := time.Duration(rand.Int63n(5))
					time.Sleep(duration * time.Second)
					return m.ConsumeSuccess
				},
			},
		},
		//Can be omitted,Created by default ConsumerConfig Topic MessageQueueLength 5 MessageCapLength 5
		ToPicConfigs: []*m.ToPicConfig{
			{
				TopicName:          "topic-test",
				MessageQueueLength: 2,
				MessageCapLength:   2,
			},
		},
	}
	mq := config.New()
	consumer := mq.Consumer
	producer := mq.Producer
	listener := mq.MonitorListener
	listener.TurnMonitor()
	for i := 0; i < 100; i++ {
		func(i int) {
			err := producer.Send(m.Message{
				Topic: "topic-test",
				Body:  []byte(strconv.Itoa(i)),
			})
			if err != nil {
				return
			}
		}(i)
	}
	//发送完毕取消订阅
	consumer.Unsubscribe("topic-test")
	//生产者关闭
	producer.Shutdown()
	//消费者关闭
	consumer.Shutdown()
	//关闭监控
	listener.CloseMonitor()
}
