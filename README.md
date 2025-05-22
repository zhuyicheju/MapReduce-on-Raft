# Distributed MapReduce with Raft Consensus

本项目融合了 [MIT 6.5840](https://pdos.csail.mit.edu/6.5840/) 的两个核心模块（详见我的主页） —— **MapReduce 任务调度框架** 和 **Raft 一致性协议**，打造出一个支持高可用、去中心化的 **分布式 MapReduce 系统**。

在传统 MapReduce 架构中，`master` 是单点故障的瓶颈；本项目通过 Raft 实现 `master` 的一致性复制与选主机制，使得系统在部分节点宕机的情况下仍可保持服务的可用性和正确性。

---

## 项目来源与构成

- **课程基础**：基于 6.5840 的 MapReduce 与 Raft 实验；
- **语言实现**：使用 Go 编写，采用标准库 `net/rpc` 替换课程中的 `labrpc`；
- **网络模型**：基于 TCP 的长连接通信，支持节点间 RPC 持久连接与自动重连。

---

## 核心功能

### 1. Raft 一致性模块

- 实现日志复制、选主、心跳和重试机制；
- 支持以下持久化状态：
  - 当前任期（`term`）
  - 投票记录（`vote`）
  - 日志条目（`log`）
- 提供 `Start()` 接口供上层应用写入指令；
- 使用 `applyCh` 通道将日志提交同步至状态机；
- 支持 Leader 崩溃后的快速恢复机制。

### 2. MapReduce 调度模块

- `Master` 节点将 Map / Reduce 任务写入 Raft 日志；
- `Worker` 向当前 `Leader` 发起任务申请；
- 任务状态仅在日志被 commit 后才会被标记为“已分配”；
- `Worker` 上报任务完成情况，`Master` 通过 Raft 再次写入更新状态；
- 内置任务重试机制：`Worker` crash 或超时自动重新调度。

### 3. 网络通信与容错

- 通信机制基于标准库 `net/rpc` 完整重写；
- 自定义连接池，实现 TCP 长连接复用；
- `Leader` 切换后自动重试逻辑；
- 异常情况下支持自动重连。

---

##  亮点与扩展设计

###  多 Master 选主架构

通过 Raft 实现多个 `Master` 节点的共识副本，避免传统 MapReduce 中单点失效问题。

###  应用层幂等机制

采用 `(ClientId, RequestId)` 机制，防止任务被重复分配或执行。

###  日志级别动态调试

提供详细日志输出，便于调试与性能分析：

- `Leader` 选举全过程；
- 每次任务写入 Raft 日志的 `Index`；
- `applyCh` 中被提交的命令序列；
- 节点之间的 RPC 请求与应答记录。

###  用户可插拔的 Map/Reduce 逻辑

支持用户自定义 `map/reduce` 逻辑：

- 将自定义逻辑文件放入 `mrapps/` 目录；
- 修改启动配置即可运行新的任务。

---

