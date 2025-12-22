package rpc

import (
	"log"
	"net/rpc"
	"sync"

	"github.com/khyallin/shardkv/config"
)

type Client struct {
	server string
	mu     sync.Mutex
	conn   *rpc.Client // lazy connection
}

func NewClient(server string) *Client {
	return &Client{
		server: server,
	}
}

func (c *Client) Call(method string, args any, reply any) bool {
	c.mu.Lock()
	if c.conn == nil {
		conn, err := rpc.Dial("tcp", c.server+config.Port)
		if err != nil {
			log.Fatal("Dial error:", err)
			c.mu.Unlock()
			return false
		}
		c.conn = conn
	}
	c.mu.Unlock()

	err := c.conn.Call(method, args, reply)
	if err != nil {
		c.conn.Close()
		c.conn = nil
		log.Printf("Call error: %v", err)
		return false
	}
	return err == nil
}
