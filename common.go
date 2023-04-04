package SheXiang_mq

type ServiceState int
type ConsumeConcurrentlyStatus int

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
