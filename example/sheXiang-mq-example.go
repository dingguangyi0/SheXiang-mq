package main

import (
	"fmt"
	m "github.com/dingguangyi0/SheXiang-mq"
	"math/rand"
	"strconv"
	"time"
)

func main() {
	//init mq config file
	config := &m.MqConfig{
		ConsumerConfig: &m.ConsumerConfig{
			MessageListeners: map[string]func(message m.Message) m.ConsumeConcurrentlyStatus{
				"topic-test": func(message m.Message) m.ConsumeConcurrentlyStatus {
					duration := time.Duration(rand.Int63n(5))
					time.Sleep(duration * time.Second)
					return m.ConsumeSuccess
				},
				"topic-test2": func(message m.Message) m.ConsumeConcurrentlyStatus {
					duration := time.Duration(rand.Int63n(5))
					time.Sleep(duration * time.Second)
					return m.ConsumeSuccess
				},
			},
		},
		Selector: m.Random,
	}
	mq, _ := config.NewStart()
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
		func(i int) {
			err := producer.Send(m.Message{
				Topic: "topic-test2",
				Body:  []byte(strconv.Itoa(i)),
			})
			if err != nil {
				return
			}
		}(i)
	}

	//发送完毕取消订阅
	consumer.Unsubscribe("topic-test", "topic-test2")
	//生产者关闭
	producer.Shutdown()
	//消费者关闭
	consumer.ShutdownCallback(func() {
		fmt.Println("消费者关闭")
	})
	//执行回调
	//关闭监控
	listener.CloseMonitor()
}
