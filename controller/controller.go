package controller

import (
	"log"

	"github.com/khyallin/shardkv/api"
	"github.com/khyallin/shardkv/config"
	"github.com/khyallin/shardkv/internal/group"
)

type Controller struct {
	*group.Clerk
	grpClerks map[config.Tgid]*group.Clerk
}

// Make a Controller, which stores its state in a kvsrv.
func MakeController(servers []string) *Controller {
	sck := &Controller{
		Clerk:     group.MakeClerk(config.Gid0, servers),
		grpClerks: make(map[config.Tgid]*group.Clerk),
	}
	return sck
}

// Please Call InitController() before starting a new controller
func (sck *Controller) InitController() {
	log.Printf("Controller.InitController()|Start")

	new, newVersion := sck.getNextConfig()
	if new == nil {
		return
	}
	old, oldVersion := sck.getCurConfig()

	ok := sck.moveShards(old, new)
	if !ok {
		log.Printf("Controller.InitController()|Fail|To=%s", new.String())
		return
	}

	err := sck.putCurConfig(new, oldVersion)
	if err == api.ErrVersion {
		log.Printf("Controller.InitController()|InProgress|To=%s", new.String())
		return
	}

	sck.deleteNextConfig(newVersion)
	log.Printf("Controller.InitController()|End|To=%s", new.String())
}

// Called once by the tester to supply the first configuration.  You
// can marshal Config into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *Controller) InitConfig(cfg *config.Config) {
	for {
		err := sck.Put("config", cfg.String(), 0, new(group.Retry))
		if err == api.ErrMaybe {
			value, _, err := sck.Get("config")
			if err == api.OK && value == cfg.String() {
				break
			}
			continue
		}
		if err == api.OK {
			break
		}
	}
	for shard := range config.NShards {
		gid, srv, _ := cfg.GidServers(config.Tshid(shard))
		clerk := group.MakeClerk(gid, srv)
		err := clerk.InstallShard(config.Tshid(shard), make([]byte, 0), cfg.Num)
		if err != api.OK {
			log.Printf("Controller.InitConfig()|InstallShard|Fail|shard=%d|gid=%d|err=%v", shard, gid, err)
		}
	}
	log.Printf("Controller.InitConfig()|OK|Config=%s", cfg.String())
}

// To change the configuration from the current one to new.
func (sck *Controller) ChangeConfigTo(new *config.Config) {
	old, oldVersion := sck.getCurConfig()
	log.Printf("Controller.ChangeConfigTo()|Start|From=%s|To=%s", old.String(), new.String())

	next, newVersion := sck.getNextConfig()
	if next != nil {
		log.Printf("Controller.ChangeConfigTo()|InProgress|To=%s", new.String())
		return
	}
	err := sck.putNextConfig(new, newVersion)
	if err == api.ErrVersion {
		log.Printf("Controller.ChangeConfigTo()|InProgress|To=%s", new.String())
		return
	}
	newVersion++

	ok := sck.moveShards(old, new)
	if !ok {
		log.Printf("Controller.ChangeConfigTo()|Fail|To=%s", new.String())
		return
	}

	err = sck.putCurConfig(new, oldVersion)
	if err == api.ErrVersion {
		log.Printf("Controller.ChangeConfigTo()|InProgress|To=%s", new.String())
		return
	}

	sck.deleteNextConfig(newVersion)
	log.Printf("Controller.ChangeConfigTo()|End|To=%s", new.String())
}

// Return the current configuration
func (sck *Controller) Query() *config.Config {
	cfg, _ := sck.getCurConfig()
	return cfg
}

func (sck *Controller) moveShards(old, new *config.Config) bool {
	log.Printf("Controller.moveShards()|From=%s|To=%s", old.String(), new.String())
	for i := 0; i < config.NShards; i++ {
		shard := config.Tshid(i)
		oldGid, oldsrv, _ := old.GidServers(shard)
		newGid, newsrv, _ := new.GidServers(shard)
		if oldGid == newGid {
			continue
		}

		oldClerk, ok := sck.grpClerks[oldGid]
		if !ok {
			oldClerk = group.MakeClerk(oldGid, oldsrv)
			sck.grpClerks[oldGid] = oldClerk
		}
		newClerk, ok := sck.grpClerks[newGid]
		if !ok {
			newClerk = group.MakeClerk(newGid, newsrv)
			sck.grpClerks[newGid] = newClerk
		}

		state, _, err := oldClerk.FreezeShard(shard, new.Num)
		if err != api.OK {
			log.Printf("Controller.moveShards()|FreezeShard|Fail|shard=%d|err=%v", shard, err)
			return false
		}
		err = newClerk.InstallShard(shard, state, new.Num)
		if err != api.OK {
			log.Printf("Controller.moveShards()|InstallShard|Fail|shard=%d|err=%v", shard, err)
			return false
		}
		err = oldClerk.DeleteShard(shard, new.Num)
		if err != api.OK {
			log.Printf("Controller.moveShards()|DeleteShard|Fail|shard=%d|err=%v", shard, err)
			return false
		}
	}
	return true
}

func (sck *Controller) getCurConfig() (*config.Config, api.Tversion) {
	value, version, err := sck.Get("config")
	if err != api.OK {
		log.Fatalf("Controller.getCurConfig()|Get|Fail|err=%v", err)
	}
	cfg := config.FromString(value)
	log.Printf("Controller.getCurConfig()|OK|Config=%s", cfg.String())
	return cfg, version
}

func (sck *Controller) getNextConfig() (*config.Config, api.Tversion) {
	value, version, err := sck.Get("next")
	if err == api.ErrNoKey || value == "" {
		return nil, version
	}
	if err != api.OK {
		log.Fatalf("Controller.getNextConfig()|Fail|err=%v", err)
	}
	cfg := config.FromString(value)
	log.Printf("Controller.getNextConfig()|OK|Config=%s", cfg.String())
	return cfg, version
}

func (sck *Controller) putCurConfig(cfg *config.Config, version api.Tversion) api.Err {
	log.Printf("Controller.putCurConfig()|Start|cfg=%s|version=%d", cfg.String(), version)
	value := cfg.String()
	err := sck.Put("config", value, version, new(group.Retry))
	if err == api.ErrMaybe {
		val, _, err := sck.Get("config")
		if err != api.OK {
			log.Fatalf("Controller.putCurConfig()|Fail")
		}
		if value == val {
			err = api.OK
		} else {
			err = api.ErrVersion
		}
	}
	log.Printf("Controller.putCurConfig()|End|cfg=%s|version=%d|err=%v", cfg.String(), version, err)
	return err
}

func (sck *Controller) putNextConfig(cfg *config.Config, version api.Tversion) api.Err {
	log.Printf("Controller.putNextConfig()|Start|cfg=%s|version=%d", cfg.String(), version)
	value := cfg.String()
	err := sck.Put("next", value, version, new(group.Retry))
	if err == api.ErrMaybe {
		val, _, err := sck.Get("next")
		if err != api.OK {
			log.Fatalf("")
		}
		if value == val {
			err = api.OK
		} else {
			err = api.ErrVersion
		}
	}
	log.Printf("Controller.putNextConfig()|End|cfg=%s|version=%d|err=%v", cfg.String(), version, err)
	return err
}

func (sck *Controller) deleteNextConfig(version api.Tversion) api.Err {
	log.Printf("Controller.deleteNextConfig()|Start|version=%d", version)
	err := sck.Put("next", "", version, new(group.Retry))
	if err == api.ErrMaybe {
		value, _, err := sck.Get("next")
		if err != api.OK {
			log.Fatalf("")
		}
		if value == "" {
			err = api.OK
		} else {
			err = api.ErrVersion
		}
	}
	log.Printf("Controller.deleteNextConfig()|End|err=%v", err)
	return err
}
