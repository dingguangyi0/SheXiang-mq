package SheXiang_mq

import (
	"errors"
	"fmt"
)

var (
	TopicEmpty = errors.New("please specify a topic to send messages ")

	ServiceStateNoStart = errors.New("producer service start createJust for not send messages")
)

type (
	//Producer Message
	Producer interface {
		Start() error

		Shutdown()

		Send(msg Message) error
	}

	DefaultProducer struct {
		ProducerGroup string
		ServiceState  ServiceState
		MqFactory     *MqFactory
	}

	ProducerConfig struct {
		ProducerGroupName string
	}
)

func newProducer(producerGroup string, factory *MqFactory) Producer {
	d := DefaultProducer{
		MqFactory:     factory,
		ServiceState:  CreateJust,
		ProducerGroup: producerGroup,
	}
	return &d
}

func (p *DefaultProducer) Start() error {
	switch p.ServiceState {
	case CreateJust:
		p.ServiceState = StartFailed

		p.checkConfig()

		p.MqFactory.registerProducer(p.ProducerGroup, p)

		p.ServiceState = Running
	case Running:
	case StartFailed:
	case ShutdownAlready:
		return ErrShutdownAlready
	default:
	}
	return nil
}

func (p *DefaultProducer) Shutdown() {
	switch p.ServiceState {
	case CreateJust:
	case Running:
		p.MqFactory.unregisterProducer(p.ProducerGroup)
		p.ServiceState = ShutdownAlready
	case StartFailed:
	case ShutdownAlready:
	default:
	}
}

func (p *DefaultProducer) Send(msg Message) error {
	if p.ServiceState != Running {
		fmt.Println("waiting for service to start...")
		return ServiceStateNoStart
	}
	err := p.checkMessage(msg)
	if err != nil {
		return err
	}
	topicPublishInfo := p.MqFactory.tryToFindTopicPublishInfo(msg.Topic)
	if topicPublishInfo != nil {
		queue := topicPublishInfo.selectOneMessageQueue()
		queue.Message <- msg
	}
	return nil
}

func (p *DefaultProducer) checkConfig() {
	if p.ProducerGroup == "" {
		fmt.Println("Please specify a producer group name for the producer group to connect to.")
	}
}

func (p *DefaultProducer) checkMessage(msg Message) error {
	if msg.Topic == "" {
		fmt.Println("Please specify a topic to send messages ")
		return TopicEmpty
	}
	return nil
}

func (p *DefaultProducer) selectOneMessageQueue(info *TopicPublishInfo) *MessageQueue {
	return p.MqFactory.selectOneMessageQueue(info)
}
