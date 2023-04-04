package SheXiang_mq

type (
	//Producer Message
	Producer interface {
		Start() error

		Shutdown()

		Send(msg Message) error
	}

	DefaultProducer struct {
		config        ProducerConfig
		ProducerGroup string
		ServiceState  ServiceState
		mqFactory     *mqFactory
	}

	ProducerConfig struct {
		ProducerGroupName string
	}
)

func newProducer(producerGroup string, factory *mqFactory) Producer {
	d := DefaultProducer{
		mqFactory:     factory,
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

		p.mqFactory.registerProducer(p.ProducerGroup, p)

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
		p.mqFactory.unregisterProducer(p.ProducerGroup)
		p.ServiceState = ShutdownAlready
	case StartFailed:
	case ShutdownAlready:
	default:
	}
}

func (p *DefaultProducer) Send(msg Message) error {
	p.checkMessage(msg)
	topicPublishInfo := p.mqFactory.tryToFindTopicPublishInfo(msg.Topic)
	if topicPublishInfo != nil {
		queue := p.selectOneMessageQueue(topicPublishInfo)
		queue.Message <- msg
	}
	return nil
}

func (p *DefaultProducer) checkConfig() {

}

func (p *DefaultProducer) checkMessage(msg Message) {

}

func (p *DefaultProducer) selectOneMessageQueue(info *TopicPublishInfo) *MessageQueue {
	return p.mqFactory.selectOneMessageQueue(info)
}
