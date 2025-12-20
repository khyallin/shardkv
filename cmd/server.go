package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/internal/group"
	"github.com/khyallin/shardkv/internal/rpc"
)

type State int

const (
	Initial State = iota
	Running
	Shutdown
)

type Server struct {
	*sync.Mutex
	state    State
	svr      *rpc.Server
	services []group.Service
}

func NewServer() *Server {
	return &Server{
		state: Initial,
		svr:   rpc.NewServer(),
	}
}

func (s *Server) Ping(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

func (s *Server) Shutdown(c *gin.Context) {
	s.Lock()
	defer s.Unlock()

	if s.state == Running {
		go func() {
			s.Lock()
			defer s.Unlock()

			s.state = Shutdown
			for _, service := range s.services {
				service.Kill()
			}
			time.Sleep(time.Second)
			os.Exit(0)
		}()
	}
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

type StartRequest struct {
	GroupId int      `json:"group_id"`
	Me      int      `json:"me"`
	Servers []string `json:"servers"`
}

func (s *Server) Start(c *gin.Context) {
	s.Lock()
	defer s.Unlock()

	if s.state == Initial {
		s.state = Running
		var req StartRequest
		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}
		s.services = group.MakeKVServer(req.Servers, config.Tgid(req.GroupId), req.Me, config.Maxraftstate)
		s.svr.Start()
	}
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

func startServer(args []string) {
	if os.Getenv("DEBUG") != "1" {
		log.SetOutput(io.Discard)
	}

	svr := NewServer()
	router := gin.Default()
	router.GET("/ping", svr.Ping)
	router.POST("/start", svr.Start)
	router.POST("/shutdown", svr.Shutdown)
	router.Run()
}
