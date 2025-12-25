package main

import (
	"flag"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/khyallin/shardkv/config"
)

type Environment struct {
	Debug   bool
	Mode    string
	GroupId config.Tgid
	Me      int
	Servers []string
}

var env = Environment{
	Debug:   false,
	Mode:    "",
	GroupId: -1,
	Me:      -1,
	Servers: nil,
}

func init() {
	if os.Getenv("DEBUG") == "1" {
		env.Debug = true
	} else {
		log.SetOutput(io.Discard)
	}

	if len(os.Args) >= 2 {
		env.Mode = os.Args[1]
	}

	if len(os.Args) < 3 {
		return
	}

	gidPtr := flag.Int("gid", -1, "Group ID")
	mePtr := flag.Int("me", -1, "Server index within group")
	servers := flag.String("servers", "", "comma separated server list")
	flag.CommandLine.Parse(os.Args[2:])

	if *gidPtr != -1 {
		env.GroupId = config.Tgid(*gidPtr)
	} else {
		if gidStr := os.Getenv("GID"); gidStr != "" {
			gid, err := strconv.Atoi(gidStr)
			if err == nil {
				env.GroupId = config.Tgid(gid)
			}
		}
	}
	if *mePtr != -1 {
		env.Me = *mePtr
	} else {
		if meStr := os.Getenv("ME"); meStr != "" {
			me, err := strconv.Atoi(meStr)
			if err == nil {
				env.Me = me
			}
		}
	}
	if *servers != "" {
		env.Servers = strings.Split(*servers, ",")
	} else {
		if srvStr := os.Getenv("SERVERS"); srvStr != "" {
			env.Servers = strings.Split(srvStr, ",")
		}
	}
}
