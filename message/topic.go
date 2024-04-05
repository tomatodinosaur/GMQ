package message

import (
	"gmq/queue"
	"gmq/util"
	"log"
)

/*
Topic的作用是接受客户端的消息，然后同时发送给所有绑定的channel上
所以它的设计和channel很类似
*/
type Topic struct {
	//名称
	name string
	//新增channel的管道
	newChannelChan chan util.ChanReq
	//维护的channel集合
	channelMap map[string]*Channel

	//接受消息的管道
	incomingMessageChan chan *Message
	//有缓冲管道，消息的内存队列
	msgChan chan *Message

	//配合使用保证channelMap的并发安全
	readSyncChan   chan struct{}
	routerSyncChan chan struct{}

	//接受退出信号的管道
	exitChan chan util.ChanReq
	//是否已向channel发送消息
	channelWriteStarted bool
	backend             queue.Queue
}

var (
	//当前Topic表
	TopicMap = make(map[string]*Topic)
	//待生产Topic任务队列
	newTopicChan = make(chan util.ChanReq)
)

func NewTopic(name string, inMemSize int) *Topic {
	topic := &Topic{
		name:                name,
		newChannelChan:      make(chan util.ChanReq),
		channelMap:          make(map[string]*Channel),
		incomingMessageChan: make(chan *Message),
		msgChan:             make(chan *Message),
		readSyncChan:        make(chan struct{}),
		routerSyncChan:      make(chan struct{}),
		exitChan:            make(chan util.ChanReq),
		backend:             queue.NewDiskQueue(name),
	}
	go topic.Router(inMemSize)
	return topic
}

// 异步请求生产topic
// 将任务发给待生产Topoc队列，并携带返回通道
// 监听返回通道
func GetTopic(name string) *Topic {
	topicChan := make(chan interface{})
	newTopicChan <- util.ChanReq{
		Variable: name,
		RetChan:  topicChan,
	}
	return (<-topicChan).(*Topic)
}

func TopicFactory(inMemSize int) {
	var (
		topicReq util.ChanReq
		name     string
		topic    *Topic
		ok       bool
	)
	for {
		//从待生产Topic任务队列取出任务，若不存在，则生产
		topicReq = <-newTopicChan
		name = topicReq.Variable.(string)
		if topic, ok = TopicMap[name]; !ok {
			topic = NewTopic(name, inMemSize)
			TopicMap[name] = topic
			log.Printf("TOPIC %s CREATED", name)
		}
		//返回给请求Topic线程
		topicReq.RetChan <- topic
	}
}

func (t *Topic) GetChannel(ChannelName string) *Channel {
	channelRet := make(chan interface{})
	t.newChannelChan <- util.ChanReq{
		Variable: ChannelName,
		RetChan:  channelRet,
	}
	return (<-channelRet).(*Channel)
}

func (t *Topic) Router(inMemSize int) {
	var closeChan = make(chan struct{})
	for {
		select {

		case channelReq := <-t.newChannelChan:
			channelName := channelReq.Variable.(string)
			channel, ok := t.channelMap[channelName]
			if !ok {
				channel = NewChannel(channelName, inMemSize)
				t.channelMap[channelName] = channel
				log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
			}
			channelReq.RetChan <- channel
			if !t.channelWriteStarted {
				go t.MessagePump(closeChan)
				t.channelWriteStarted = true
			}

		case msg := <-t.incomingMessageChan:
			select {
			case t.msgChan <- msg:
				log.Printf("TOPIC(%s) wrote message", t.name)
			default:
				err := t.backend.Put(msg.data)
				if err != nil {
					log.Printf("ERROR: t.backend.Put() - %s", err.Error())
				}
				log.Printf("TOPIC(%s): wrote to backend", t.name)
			}

		case <-t.readSyncChan:
			<-t.routerSyncChan

		case closeReq := <-t.exitChan:
			log.Printf("TOPIC(%s): closing", t.name)
			for _, channel := range t.channelMap {
				err := channel.Close()
				if err != nil {
					log.Printf("ERROR: channel(%s) close - %s", channel.name, err.Error())
				}
			}
			close(closeChan)
			closeReq.RetChan <- t.backend.Close()
		}
	}
}

// 推送消息给 channel
func (t *Topic) PutMessage(msg *Message) {
	t.incomingMessageChan <- msg
}

// 将消息推送多份给绑定的所有channel
func (t *Topic) MessagePump(closeChan <-chan struct{}) {
	var msg *Message
	for {
		select {
		case msg = <-t.msgChan:
		case <-t.backend.ReadReadyChan():
			bytes, err := t.backend.Get()
			if err != nil {
				log.Printf("ERROR: t.backend.Get() - %s", err.Error())
				continue
			}
			msg = NewMessage(bytes)
		case <-closeChan:
			return
		}
		t.readSyncChan <- struct{}{}
		for _, channel := range t.channelMap {
			go func(ch *Channel) {
				ch.PutMessage(msg)
			}(channel)
		}
		t.routerSyncChan <- struct{}{}
	}
}

func (t *Topic) Close() error {
	errChan := make(chan interface{})
	t.exitChan <- util.ChanReq{
		RetChan: errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}
