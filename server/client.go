package server

import (
	"context"
	"encoding/binary"
	"gmq/protocol"
	"io"
	"log"
)

type Client struct {
	conn  io.ReadWriteCloser
	name  string
	state int
}

func NewClient(conn io.ReadWriteCloser, name string) *Client {
	return &Client{conn, name, -1}
}

func (c *Client) String() string {
	return c.name
}

func (c *Client) GetState() int {
	return c.state
}

func (c *Client) SetState(state int) {
	c.state = state
}

func (c *Client) Read(data []byte) (int, error) {
	return c.conn.Read(data)
}

// 再给消费者写信息之前，先往连接中写入消息体的长度，
// 固定位4个字节，这样客户端读取的时候就可以先读取长度
// 然后按长度读取信息
func (c *Client) Write(data []byte) (int, error) {
	var err error
	err = binary.Write(c.conn, binary.BigEndian, int32(len(data)))

	if err != nil {
		return 0, err
	}

	n, err := c.conn.Write(data)
	if err != nil {
		return 0, err
	}

	return n + 4, nil
}

func (c *Client) Close() {
	log.Printf("CLIENT(%s):closing", c.String())
	c.conn.Close()
}

func (c *Client) Handle(ctx context.Context) {
	defer c.Close()
	proto := &protocol.Protocol{}
	err := proto.IOLoop(ctx, c)
	if err != nil {
		log.Printf("ERROR: client(%s) - %s", c.String(), err.Error())
		return
	}
}
