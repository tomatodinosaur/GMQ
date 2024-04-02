package protocol

import (
	"bufio"
	"gmq/message"
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
		return err
	}

}

func (p *Protocol) Execute(client SatefulReadWriter, params ...string) ([]byte, error) {
	var (
		err  error
		resp []byte
	)

	typ := reflect.TypeOf(p)
}
