package protocol

import (
	"bufio"
	"gmq/message"
	"gmq/util"
	"log"
	"reflect"
	"strings"
)

/*
协议:就是规定了消费者的行为，
并将这些行为转化成对 topic、channel 或者 message 的操作。
例如，客户端发送 SUB order pay，
我们就创建一个名为 order 的 topic，
再在 topic 下面创建一个名为 pay 的 channel，
最后将该客户端与该 channel 绑定，
后续该客户端就能接收到生产者的消息了

	SUB（订阅）、GET（读取）、FIN（完成）和 REQ （重入）
*/

type Protocol struct {
	channel *message.Channel
}

func (p *Protocol) IOLoop(client SatefulReadWriter) error {
	var (
		err  error
		line string
		resp []byte
	)
	client.SetState(ClientInit)
	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.Replace(line, "\n", "", -1)
		line = strings.Replace(line, "\r", "", -1)
		params := strings.Split(line, " ")
		log.Printf("PROTOCOL: %#v", params)

		resp, err = p.Execute(client, params...)
		if err != nil {
			_, err = client.Write([]byte(err.Error()))
			if err != nil {
				break
			}
			continue
		}
		if resp != nil {
			_, err = client.Write(resp)
			if err != nil {
				break
			}
		}
	}
	return err
}

/*
以传入的 params 的第一项作为方法名，判断有无实现该函数并执行发射调用。
例如客户端发送 SUB order pay，就
是先判断 &Protocol 有没有实现 SUB 方法，
有的话就将 client 和 params 数组一起作为参数传给 SUB 方法执行调用，
并返回调用结果。
*/
func (p *Protocol) Execute(client SatefulReadWriter, params ...string) ([]byte, error) {
	var (
		err  error
		resp []byte
	)

	typ := reflect.TypeOf(p)
	args := make([]reflect.Value, 3)
	args[0] = reflect.ValueOf(p)
	args[1] = reflect.ValueOf(client)

	cmd := strings.ToUpper(params[0])

	if method, ok := typ.MethodByName(cmd); ok {
		args[2] = reflect.ValueOf(params)
		returnValues := method.Func.Call(args)

		if !returnValues[0].IsNil() {
			resp = returnValues[0].Interface().([]byte)
		}
		if !returnValues[1].IsNil() {
			err = returnValues[1].Interface().(error)
		}
		return resp, err
	}
	return nil, &ClientErrInvalid
}

// 订阅
// 获取 topic，再获取 channel，最后绑定客户端连接和 channel。
func (p *Protocol) SUB(client SatefulReadWriter, params []string) ([]byte, error) {
	if client.GetState() != ClientInit {
		return nil, &ClientErrInvalid
	}

	if len(params) < 3 {
		return nil, &ClientErrInvalid
	}

	topicName := params[1]
	if len(topicName) == 0 {
		return nil, &ClientErrBadTopic
	}

	channelName := params[2]
	if len(channelName) == 0 {
		return nil, &ClientErrBadChannel
	}

	client.SetState(ClientWaitGet)

	topic := message.GetTopic(topicName)
	p.channel = topic.GetChannel(channelName)

	return nil, nil
}

// 读取
// 向绑定的 channel 发送消息，然后修改状态
func (p *Protocol) GET(client SatefulReadWriter, params []string) ([]byte, error) {
	if client.GetState() != ClientWaitGet {
		return nil, &ClientErrInvalid
	}
	msg := p.channel.PullMessage()
	if msg == nil {
		log.Printf("ERROR: msg==nil")
		return nil, &ClientErrBadMessage
	}

	uuidStr := util.UuidToStr(msg.Uuid())
	log.Printf("PROTOCOL: writing msg(%s) to client(%s) - %s", uuidStr, client.String(), string(msg.Body()))

	client.SetState(ClientWaitResponse)
	return msg.Data(), nil
}

// 完成
func (p *Protocol) FIN(client SatefulReadWriter, params []string) ([]byte, error) {
	if client.GetState() != ClientWaitResponse {
		return nil, &ClientErrInvalid
	}

	if len(params) < 2 {
		return nil, &ClientErrInvalid
	}

	uuidStr := params[1]
	err := p.channel.FinishMessage(uuidStr)
	if err != nil {
		return nil, err
	}

	client.SetState(ClientWaitGet)
	return nil, nil
}

// 重传
func (p *Protocol) REQ(client SatefulReadWriter, params []string) ([]byte, error) {
	if client.GetState() != ClientWaitResponse {
		return nil, &ClientErrInvalid
	}

	if len(params) < 2 {
		return nil, &ClientErrInvalid
	}

	uuidStr := params[1]
	err := p.channel.RequeueMessage(uuidStr)
	if err != nil {
		return nil, err
	}

	client.SetState(ClientWaitGet)
	return nil, nil
}
