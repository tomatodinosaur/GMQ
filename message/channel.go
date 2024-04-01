package message

import (
	"errors"
	"gmq/util"
	"log"
	"time"
)

/*
这里我们没有直接绑定上面的 Client 结构体，
而是抽象出了一个 Consumer 接口。
这样做的好处是倒转依赖关系，而且可以避免包循环引用。
*/
type Consumer interface {
	Close()
}

type Channel struct {
	name string

	addClientChan    chan util.ChanReq
	removeClientChan chan util.ChanReq
	clients          []Consumer

	incomingMessageChan chan *Message
	msgChan             chan *Message
	clientMessageChan   chan *Message

	exitChan chan util.ChanReq

	inFlightMessageChan chan *Message
	inFlightMessages    map[string]*Message
	finishMessageChan   chan util.ChanReq

	requeueMessageChan chan util.ChanReq
}

func NewChannel(name string, inMemSize int) *Channel {
	channel := &Channel{
		name:                name,
		addClientChan:       make(chan util.ChanReq),
		removeClientChan:    make(chan util.ChanReq),
		clients:             make([]Consumer, 0, 5),
		incomingMessageChan: make(chan *Message, 5),
		msgChan:             make(chan *Message, inMemSize),
		clientMessageChan:   make(chan *Message),
		exitChan:            make(chan util.ChanReq),
		inFlightMessageChan: make(chan *Message),
		inFlightMessages:    make(map[string]*Message),
		requeueMessageChan:  make(chan util.ChanReq),
		finishMessageChan:   make(chan util.ChanReq),
	}
	go channel.Router()
	return channel
}

func (c *Channel) Close() error {
	errChan := make(chan interface{})
	c.exitChan <- util.ChanReq{
		RetChan: errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}

/*
	 维护消费者信息
		AddClient、RemoveClient
*/
func (c *Channel) AddClient(client Consumer) {
	log.Printf("Channel(%s): adding client...", c.name)
	doneChan := make(chan interface{})
	c.addClientChan <- util.ChanReq{
		Variable: client,
		RetChan:  doneChan,
	}
	<-doneChan

}

func (c *Channel) RemoveClient(client Consumer) {
	log.Printf("Channel(%s): removing client...", c.name)
	doneChan := make(chan interface{})
	c.removeClientChan <- util.ChanReq{
		Variable: client,
		RetChan:  doneChan,
	}
	<-doneChan

}

/*
收发信息
msgChan：这是一个有缓冲管道，用来暂存消息，超过长度则丢弃消息（后续会加上持久化到磁盘的功能）
incomingMessageChan：用来接收生产者的消息
clientMessageChan：消息会被发送到这个管道，后续会由消费者拉取
*/
func (c *Channel) PutMessage(msg *Message) {
	c.incomingMessageChan <- msg
}

func (c *Channel) PullMessage() *Message {
	return <-c.incomingMessageChan
}

/*
At least once 机制
*/
func (c *Channel) pushInFlightMessage(msg *Message) {
	c.inFlightMessages[util.UuidToStr(msg.Uuid())] = msg
}

func (c *Channel) popInFlightMessage(uuidStr string) (*Message, error) {
	msg, ok := c.inFlightMessages[uuidStr]
	if !ok {
		return nil, errors.New("UUID not in flight")
	}
	delete(c.inFlightMessages, uuidStr)
	//消息结束，停止重传计时器
	msg.EndTimer()
	return msg, nil
}

func (c *Channel) FinishMessage(uuidStr string) error {
	errChan := make(chan interface{})
	c.finishMessageChan <- util.ChanReq{
		Variable: uuidStr,
		RetChan:  errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}

/*
重新入队
*/
func (c *Channel) RequeueMessage(uuidStr string) error {
	errChan := make(chan interface{})
	c.requeueMessageChan <- util.ChanReq{
		Variable: uuidStr,
		RetChan:  errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}

// 负责监听 确认通道 和 重传通道
func (c *Channel) RequeueRouter(closeChan chan struct{}) {
	for {
		select {

		case msg := <-c.inFlightMessageChan:
			c.pushInFlightMessage(msg)
			go func(msg *Message) {
				select {
				case <-time.After(60 * time.Second):
					log.Printf("CHANNEL(%s): auto requeue of message(%s)", c.name, util.UuidToStr(msg.Uuid()))
				case <-msg.finishChan:
					return
				}
			}(msg)

		case finishReq := <-c.finishMessageChan:
			uuidStr := finishReq.Variable.(string)
			_, err := c.popInFlightMessage(uuidStr)
			if err != nil {
				log.Printf("ERROR:failed to finish message(%s) - %s", uuidStr, err.Error())
			}
			finishReq.RetChan <- err

		case requeueReq := <-c.requeueMessageChan:
			uuidStr := requeueReq.Variable.(string)
			msg, err := c.popInFlightMessage(uuidStr)
			if err != nil {
				log.Printf("ERROR:failed to requeue message(%s) - %s", uuidStr, err.Error())
			} else {
				go func(msg *Message) {
					c.PutMessage(msg)
				}(msg)
			}
			requeueReq.RetChan <- err

		case <-closeChan:
			return
		}
	}
}

func (c *Channel) Router() {
	//用于监听client队列
	var clientReq util.ChanReq

	//用于关闭MessagePump
	var closeChan = make(chan struct{})
	go c.MessagePump(closeChan)
	go c.RequeueRouter(closeChan)
	for {
		select {

		case clientReq = <-c.addClientChan:
			client := clientReq.Variable.(Consumer)
			c.clients = append(c.clients, client)
			log.Printf("CHANNEL(%s) added client %#v", c.name, client)
			clientReq.RetChan <- struct{}{}

		case clientReq = <-c.removeClientChan:
			client := clientReq.Variable.(Consumer)
			indexToRemove := -1
			for k, v := range c.clients {
				if v == client {
					indexToRemove = k
					break
				}
			}
			if indexToRemove == -1 {
				log.Printf("ERROR: could not find client(%#v) in clients(%#v)", client, c.clients)
			} else {
				c.clients = append(c.clients[:indexToRemove], c.clients[indexToRemove+1:]...)
				log.Printf("CHANNEL(%s) removed client %#v", c.name, client)
			}
			clientReq.RetChan <- struct{}{}

		//将生产者产物 缓冲到 消息缓冲区
		case msg := <-c.incomingMessageChan:
			//增加判断防止msgChan填满阻塞
			select {
			case c.msgChan <- msg:
				log.Printf("CHANNEL(%s) wrote message", c.name)
			//msgChan缓冲填满直接丢弃
			default:
			}

		//监听到channel关闭的消息
		case closeReq := <-c.exitChan:
			log.Printf("CHANNEL(%s) is closing", c.name)
			//停掉MessagePump
			// close(closeChan)
			closeChan <- struct{}{}

			for _, consumer := range c.clients {
				consumer.Close()
			}

			//通知关闭结束且无错误
			closeReq.RetChan <- nil
		}
	}
}

// MessagePump send messages to ClientMessageChan
// 从消息缓冲区 提取 生产者产物 源源不断发送到 消费者待消费队列
func (c *Channel) MessagePump(closeChan chan struct{}) {
	var msg *Message

	for {
		select {
		case msg = <-c.msgChan:

		//监听closeChan,收到关闭消息
		case <-closeChan:
			return
		}

		if msg != nil {
			c.inFlightMessageChan <- msg
		}

		c.clientMessageChan <- msg
	}
}
