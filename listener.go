package SheXiang_mq

type (
	MonitorListener interface {
		Surround(messageListener func(message Message) ConsumeConcurrentlyStatus, msg Message)

		Print()

		Info() map[string]*Msg

		TurnMonitor()

		CloseMonitor()
	}
)
