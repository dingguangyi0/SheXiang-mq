# Shexiang-mq
Shexiang-mq 充分利用go 高并发特性的本地mq
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
表示一类消息的集合，每个主题包含若干条消息，每条消息只能属于一个主题

## Examples & Demos

```go
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
			PoolSize: 10,
			MessageListeners: map[string]func(message m.Message) m.ConsumeConcurrentlyStatus{
				"topic-test": func(message m.Message) m.ConsumeConcurrentlyStatus {
					fmt.Println("Consumed topic-test")
					duration := time.Duration(rand.Int63n(5))
					time.Sleep(duration * time.Second)
					return m.ConsumeSuccess
				},
				"topic-test2": func(message m.Message) m.ConsumeConcurrentlyStatus {
					fmt.Println("Consumed topic-test2")
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
	for i := 0; i < 1; i++ {
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
	fmt.Println("Produ")

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

```
