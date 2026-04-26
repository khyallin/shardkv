# ShardKV

<div align="center">

[![Go Version](https://img.shields.io/badge/Go-1.24.11-blue.svg)](go.mod)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

一个基于 Raft 的分片键值存储实现，提供控制器、分组服务端和交互式客户端。

</div>

---

## 概述

`shardkv` 是一个分片键值存储系统，核心能力包括：

- 基于一致性复制的组内高可用（Raft + RSM）
- 基于配置的分片路由（`config.Key2Shard`）
- 分片迁移流程（Freeze / Install / Delete）
- 乐观版本控制写入（`Put(key, value, version)`）
- 组级状态指标查询（QPS、延迟）

入口二进制在 [cmd/main.go](cmd/main.go)，支持三种模式：

- `server`：启动分片组节点
- `ctrler`：启动控制器并初始化配置
- `client`：交互式命令行客户端

---

## 亮点特性

- 可视化管理平台：[shardkv-dashboard](https://github.com/khyallin/shardkv-dashboard) 让复杂的集群管理变得简单直观  
- 分片路由：键通过 FNV 哈希映射到固定分片数（`NShards=12`，见 [config/default.go](config/default.go)）。
- 分片迁移：控制器调用 `FreezeShard -> InstallShard -> DeleteShard` 完成数据搬迁（见 [controller/controller.go](controller/controller.go)）。
- 版本语义：`Put` 要求携带版本号，不匹配返回 `ErrVersion`（见 [internal/statemachine/memorykv.go](internal/statemachine/memorykv.go)）。
- 客户端容错：遇到 `ErrWrongGroup` 自动刷新配置并重试（见 [client/clerk.go](client/clerk.go)）。
- 快照裁剪：Raft 状态超过 `Maxraftstate` 时触发快照（见 [internal/rsm/rsm.go](internal/rsm/rsm.go)）。

---

## 架构

```text
+------------------------+         +--------------------------+
|   CLI client mode      |         |      ctrler mode         |
|  (cmd/client.go)       |         |   (cmd/ctrler.go)        |
+-----------+------------+         +------------+-------------+
            |                                   |
            | Get/Put/Status                    | config / next config
            v                                   v
+------------------------+         +--------------------------+
|     group.Clerk        | <-----> |   controller.Controller  |
+-----------+------------+         +------------+-------------+
            |                                   |
            | RPC (KVServer.*)                 | shard migration RPC
            v                                   v
+--------------------------------------------------------------+
|                      KVServer (group)                        |
|      rsm.RSM -> raft.Raft -> statemachine.MemoryKV          |
+--------------------------------------------------------------+
```

默认端口定义：

- 数据 RPC：`:8380`（[config/default.go](config/default.go)）
- 管理端口常量：`:8379`（已定义，当前代码中未发现对外监听实现）

---

## 快速开始

### 1) 构建

```bash
go build -o shardkv ./cmd
```

### 2) 启动一个最小单组（3 节点示例）

在 3 个终端分别执行：

```bash
./shardkv server -gid 1 -me 0 -servers 127.0.0.1,127.0.0.2,127.0.0.3
./shardkv server -gid 1 -me 1 -servers 127.0.0.1,127.0.0.2,127.0.0.3
./shardkv server -gid 1 -me 2 -servers 127.0.0.1,127.0.0.2,127.0.0.3
```

说明：

- 客户端会对 `servers[i] + :8380` 发起 RPC（见 [internal/rpc/client.go](internal/rpc/client.go)）。
- `-servers` 里只写主机名/IP，不需要带端口。

### 3) 启动控制器

```bash
./shardkv ctrler -servers 127.0.0.1,127.0.0.2,127.0.0.3
```

控制器会调用 `InitConfig(defaultConfig)` 初始化配置（见 [cmd/ctrler.go](cmd/ctrler.go)）。

### 4) 启动交互客户端

```bash
./shardkv client -servers 127.0.0.1,127.0.0.2,127.0.0.3
```

进入交互模式后可执行：

```text
put <key> <value> [version]
get <key>
exit
```

示例：

```text
> put user:1 alice 0
Put key=user:1, value=alice, version=0
> get user:1
Get key=user:1, value=alice, version=1
```

---

## 命令行用法

帮助信息（见 [cmd/main.go](cmd/main.go)）：

```bash
shardkv help
```

### server

```bash
shardkv server -gid <groupid> -me <serverindex> -servers <server1,server2,...>
```

### ctrler

```bash
shardkv ctrler -servers <server1,server2,...>
```

### client

```bash
shardkv client -servers <server1,server2,...>
```

---

## 配置与环境变量

命令行参数与环境变量都支持（见 [cmd/env.go](cmd/env.go)）：

- `-gid` 或环境变量 `GID`
- `-me` 或环境变量 `ME`
- `-servers` 或环境变量 `SERVERS`
- `DEBUG=1` 打开日志，否则日志输出会被丢弃

示例：

```bash
export DEBUG=1
export GID=1
export ME=0
export SERVERS=127.0.0.1,127.0.0.2,127.0.0.3
./shardkv server
```


## 代码结构

```text
shardkv/
├── api/                 # 请求/响应与通用错误定义
├── client/              # 面向业务的 Clerk（自动根据配置路由）
├── cmd/                 # 可执行入口：server / ctrler / client
├── config/              # 分片与集群配置结构、默认常量
├── controller/          # 配置管理与分片迁移控制器
└── internal/
    ├── group/           # KVServer 与组内 Clerk
    ├── raft/            # Raft 实现
    ├── rpc/             # RPC 客户端/服务端与迁移 RPC 定义
    ├── rsm/             # replicated state machine 封装
    └── statemachine/    # 内存 KV 状态机
```

---

## 返回错误语义

定义位于 [api/error.go](api/error.go) 与 [internal/rpc/error.go](internal/rpc/error.go)：

- `OK`
- `ErrNoKey`
- `ErrVersion`
- `ErrMaybe`（客户端语义，表示结果不确定）
- `ErrWrongLeader`
- `ErrWrongGroup`

## 作为库集成（可选）

如果你在自己的 Go 程序里使用：

```go
import "github.com/khyallin/shardkv/client"

ck := client.MakeClerk([]string{"127.0.0.1", "127.0.0.2", "127.0.0.3"})
err := ck.Put("k", "v", 0)
value, version, err := ck.Get("k")
```

也可以通过 [controller/controller.go](controller/controller.go) 中的 `ChangeConfigTo` 进行编程式配置变更。

---

## 许可证

本项目采用 [MIT License](LICENSE)。

## 致谢

算法来自课程实验
[MIT 6.5840](https://pdos.csail.mit.edu/6.824/) - 麻省理工大学 Distributed System，前身为 MIT 6.824
