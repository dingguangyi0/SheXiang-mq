package SheXiang_mq

type (
	Message struct {
		Topic      string
		Body       []byte
		Properties map[string]any
	}

	MessageQueue struct {
		QueueId string
		Message chan Message
	}
)
