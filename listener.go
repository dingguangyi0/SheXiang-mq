package SheXiang_mq

type (
	MonitorListener interface {
		Init(topic []*TopicPublishInfo)

		InitByTopic(topicInfo *TopicPublishInfo)

		surround(messageListener func(message Message) ConsumeConcurrentlyStatus, msg Message)

		Info()

		TurnMonitor()

		CloseMonitor()
	}
)
