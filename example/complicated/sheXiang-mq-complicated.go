package main

import (
	"fmt"
	m "github.com/dingguangyi0/SheXiang-mq"
	"strconv"
)

func main() {

	//init mq config file
	mq := m.Instance(&m.FactoryConfig{
		ToPicConfigs: []*m.ToPicConfig{
			m.NewToPicConfig("topic-test"),
			m.NewToPicConfig("topic-test-1"),
			m.NewToPicConfig("topic-test-2"),
		},
	})
	producer := mq.GetProducer()
	err := producer.Start()
	if err != nil {
		fmt.Println("Error creating producer", err)
		return
	}
	//listener := mq.MonitorListener
	consumer := mq.GetConsumer()
	consumer.RegisterMessageListener("topic-test", t1(producer))
	consumer.RegisterMessageListener("topic-test-1", t2(producer))
	consumer.RegisterMessageListener("topic-test-2", t3())
	err = consumer.Start()
	if err != nil {
		fmt.Println("Error registering consumer", err)
		return
	}
	fmt.Println("Monitoring")
	for i := 0; i < 20001; i++ {
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
	fmt.Println("Unsubscribe")

	//发送完毕取消订阅
	//consumer.Unsubscribe("topic-test")
	//生产者关闭
	//producer.Shutdown()
	//消费者关闭
	consumer.Shutdown()
	//关闭监控
	//listener.CloseMonitor()
}

func t3() func(message m.Message) m.ConsumeConcurrentlyStatus {
	return func(message m.Message) m.ConsumeConcurrentlyStatus {
		fmt.Println("topic-3->" + string(message.Body))
		return m.ConsumeSuccess
	}
}

func t2(p m.Producer) func(message m.Message) m.ConsumeConcurrentlyStatus {
	return func(message m.Message) m.ConsumeConcurrentlyStatus {
		fmt.Println(message.Topic, "-", string(message.Body))
		err := p.Send(m.Message{
			Topic: "topic-test-2",
			Body:  []byte(("topic-2->" + string(message.Body))),
		})
		if err != nil {
			return 0
		}
		return m.ConsumeSuccess
	}
}

func t1(p m.Producer) func(message m.Message) m.ConsumeConcurrentlyStatus {
	return func(message m.Message) m.ConsumeConcurrentlyStatus {
		fmt.Println(message.Topic, "-", string(message.Body))
		err := p.Send(m.Message{
			Topic: "topic-test-1",
			Body:  []byte(("message-" + string(message.Body))),
		})
		if err != nil {
			return 0
		}
		return m.ConsumeSuccess
	}
}
