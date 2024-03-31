package message

type Message struct {
	data []byte
}

func NewMessage(data []byte) *Message {
	return &Message{
		data: data,
	}
}

// 前16位用作消息的唯一标识
func (m *Message) Uuid() []byte {
	return m.data[:16]
}

// 消息的本身内容
func (m *Message) Body() []byte {
	return m.data[16:]
}

func (m *Message) Data() []byte {
	return m.data
}
