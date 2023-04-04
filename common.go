package SheXiang_mq

type ServiceState int
type ConsumeConcurrentlyStatus int
type MsgQueueSelector int

const (
	// CreateJust Service just created,not start
	CreateJust ServiceState = iota
	// Running Service Running
	Running
	// ShutdownAlready Service shutdown
	ShutdownAlready
	// StartFailed Service Start failure
	StartFailed
)

const (
	// ConsumeSuccess Success consumption
	ConsumeSuccess ConsumeConcurrentlyStatus = iota
	// ReconsumeLater Failure consumption,later try to consume
	ReconsumeLater
)

const (
	DefaultProducerGroup = "DEFAULT_PRODUCER"
	DefaultConsumerGroup = "DEFAULT_CONSUMER"
)

const (
	DefaultMessageQueueLength = 50
	DefaultMessageCapLength   = 20
)

const (
	// Random 随机
	Random MsgQueueSelector = iota
	Polling
)
