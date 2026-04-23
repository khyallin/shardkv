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
	conn := c.conn
	if conn == nil {
		newConn, err := rpc.Dial("tcp", c.server+config.Port)
		if err != nil {
			log.Printf("Dial error: %v", err)
			c.mu.Unlock()
			return false
		}
		c.conn, conn = newConn, newConn
	}
	c.mu.Unlock()

	err := conn.Call(method, args, reply)
	if err != nil {
		c.mu.Lock()
		if c.conn == conn {
			_ = conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()
		log.Printf("Call error: %v", err)
		return false
	}
	return err == nil
}
