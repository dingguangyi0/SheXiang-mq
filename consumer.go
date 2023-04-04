package SheXiang_mq

import (
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"sync"
	"time"
)

type (
	Consumer interface {
		// Start the consumer with the given parameters and return immediately
		Start() error

		// Shutdown Stop the consumer with the given parameters and return immediately
		Shutdown()

		ShutdownCallback(callback func())

		// Subscribe with the given parameters and return immediately
		Subscribe(topic string) bool

		// Unsubscribe with the given parameters and return immediately
		Unsubscribe(topics ...string)

		// RegisterMessageListener with the given parameters and return immediately and
		RegisterMessageListener(topic string, l func(message Message) ConsumeConcurrentlyStatus)
	}

	//AllocateMessageQueueStrategy Strategy Algorithm for message allocating between consumers
	AllocateMessageQueueStrategy interface {
		// Name The strategy name
		Name() string

		// Allocate To allocate result of given strategy
		Allocate() []MessageQueue
	}

	DefaultConsumer struct {
		ConsumerGroup string
		ServiceState  ServiceState
		Listeners     map[string]func(message Message) ConsumeConcurrentlyStatus
		topicPubInfos []*TopicPublishInfo
		mqFactory     *MqFactory
		pool          *ants.Pool
		subscribes    []string
	}

	ConsumerConfig struct {
		ConsumerGroup    string
		MessageListeners map[string] /*topicName*/ func(message Message) ConsumeConcurrentlyStatus
	}
)

func newConsumer(config *ConsumerConfig, factory *MqFactory) Consumer {
	d := DefaultConsumer{
		mqFactory:     factory,
		ServiceState:  CreateJust,
		ConsumerGroup: config.ConsumerGroup,
		Listeners:     make(map[string]func(message Message) ConsumeConcurrentlyStatus),
	}
	return &d
}

func (c *DefaultConsumer) Start() error {
	switch c.ServiceState {
	case CreateJust:
		c.ServiceState = StartFailed

		err := c.checkConfig()
		if err != nil {
			return err
		}

		c.mqFactory.registerConsumer(c.ConsumerGroup, c)

		pool, err := c.newPool()
		//线程池初始化失败
		if err != nil {
			return errors.New("create consumer failed for new pool error " + err.Error())
		}
		//初始化线程池
		c.pool = pool

		err = c.processing()

		if err != nil {
			fmt.Println("Failed to")
			return err
		}

		c.ServiceState = Running
	case Running:
	case StartFailed:
	case ShutdownAlready:
		return errors.New("the PullConsumer service state not OK, maybe started once")
	default:
	}
	return nil
}

func (c *DefaultConsumer) Shutdown() {
	c.ShutdownCallback(func() {})
}
func (c *DefaultConsumer) ShutdownCallback(callback func()) {
	switch c.ServiceState {
	case CreateJust:
	case Running:
		pool := c.pool
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				if pool.Running() == 0 && pool.Waiting() == 0 {
					c.mqFactory.unregisterConsumer(c.ConsumerGroup)
					c.ServiceState = ShutdownAlready
					ticker.Stop()
					c.pool.Release()
					callback()
					return
				}
			}
		}

	case StartFailed:
	case ShutdownAlready:
	default:
	}
}

func (c *DefaultConsumer) Subscribe(topic string) bool {
	info := c.mqFactory.tryToFindTopicPublishInfo(topic)
	if info == nil {
		return false
	}
	c.topicPubInfos = append(c.topicPubInfos, info)
	return true
}

func (c *DefaultConsumer) initTopicPublishInfo(topic string) bool {
	info := c.mqFactory.tryToFindTopicPublishInfo(topic)
	if info == nil {
		return false
	}
	c.topicPubInfos = append(c.topicPubInfos, info)
	return false
}

func (c *DefaultConsumer) processingTopicQueue(info *TopicPublishInfo) error {
	pool := c.pool
	f := c.Listeners[info.ToPicConfig.TopicName]
	if f == nil {
		return nil
	}
	c.mqFactory.monitorListener.InitByTopic(info)
	for i := 0; i < info.ToPicConfig.getMessageQueueLength(); i++ {
		//初始化监控
		func(queue *MessageQueue, config *ToPicConfig) {
			err := pool.Submit(func() {
				for m := range queue.Message {
					func(m Message, config *ToPicConfig) {
						err := pool.Submit(func() {
							c.mqFactory.monitorListener.surround(f, m)
						})
						if err != nil {
							fmt.Println(err)
							return
						}
					}(m, config)
				}
			})
			if err != nil {
				fmt.Println(err)
			}
		}(info.messageQueues[i], info.ToPicConfig)
	}
	return nil
}

func (c *DefaultConsumer) processing() error {
	var wg sync.WaitGroup
	wg.Add(len(c.topicPubInfos))
	for _, topicPubInfo := range c.topicPubInfos {
		go func(info *TopicPublishInfo) {
			err := c.processingTopicQueue(info)

			if err != nil {
				fmt.Println(err)
				return
			}
			wg.Done()
		}(topicPubInfo)
	}
	wg.Wait()
	return nil
}

func (c *DefaultConsumer) Unsubscribe(topics ...string) {
	c.checkState()
	for _, topic := range topics {
		info := c.mqFactory.TopicPublishInfoTable[topic]
		if info != nil {
			//关闭发送通道
			info.oneClose.Do(func() {
				for _, queue := range info.messageQueues {
					close(queue.Message)
				}
			})
		}
		delete(c.Listeners, topic)
	}
}

func (c *DefaultConsumer) RegisterMessageListener(topic string, listener func(message Message) ConsumeConcurrentlyStatus) {
	c.Subscribe(topic)
	c.Listeners[topic] = listener
}

func (c *DefaultConsumer) checkConfig() error {
	if len(c.topicPubInfos) == 0 {

	}
	return nil
}

func (c *DefaultConsumer) Callback(f func()) {
	f()
}

func (c *DefaultConsumer) newPool() (*ants.Pool, error) {
	poolSize := 5
	for _, config := range c.topicPubInfos {
		poolSize += config.ToPicConfig.MessageQueueLength
	}

	options := func(opts *ants.Options) {
		opts.ExpiryDuration = 10 * time.Second
		opts.DisablePurge = false
	}

	pool, err := ants.NewPool(poolSize, options)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func (c *DefaultConsumer) checkState() bool {
	if c.ServiceState == Running {
		return true
	}
	return false
}
