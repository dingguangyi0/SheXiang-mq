# Shexiang-mq
Shexiang-mq 重复利用go 高并发特性的本地mq
## 目录

- [Install](#install)
- [Usage](#usage)
- [Examples & Demos](#examples--demos)


## Install

```sh
go get -u github.com/dingguangyi0/SheXiang-mq
```


## Usage
### 1 消息模型（Message Model）

Shexiang-mq 主要由 Producer、Consumer、MessageQueue、 三部分组成，其中Producer 负责生产消息，Consumer 负责并发消费消息，MessageQueue 负责存储消息，每个Topic中的消息地址存储于多个 Message Queue 中

### 2 消息生产者（Producer）
负责生产消息，一般由业务系统负责生产消息。

### 3 消息消费者（Consumer）
负责消费消息，一般是后台系统负责异步消费。启动消费者后,消费者会启动监听 MessageQueue队列。

### 4 主题（Topic）
表示一类消息的集合，每个主题包含若干条消息，每条消息只能属于一个主题，是RocketMQ进行消息订阅的基本单位。

## Examples & Demos

```go
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

```
