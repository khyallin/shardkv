package rpc

import (
	"log"
	"net"
	"net/rpc"

	"github.com/khyallin/shardkv/internal/config"
)

type Server struct {
	server *rpc.Server
}

func NewServer() *Server {
	return &Server{
		server: rpc.NewServer(),
	}
}

func (s *Server) Register(name string, rcvr any) bool {
	err := s.server.RegisterName(name, rcvr)
	if err != nil {
		log.Fatal("Register error:", err)
		return false
	}
	return true
}

func (s *Server) Start() {
	listener, err := net.Listen("tcp", config.Port)
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatal("Accept error:", err)
			}
			go s.server.ServeConn(conn)
		}
	}()
}
