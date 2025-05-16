Distributed MapReduce with Raft Consensus

本项目融合了 6.5840 的两个核心模块(详见我的主页上) —— MapReduce 任务调度框架 和 Raft 一致性协议，打造出一个支持高可用、去中心化的 分布式 MapReduce 系统。在传统 MapReduce 架构中，master 是单点故障瓶颈；本项目通过 Raft 实现 master 的一致性复制与选主机制，使得系统在部分节点宕机的情况下仍可保持服务可用性和正确性。

项目来源与构成

    课程基础：基于 6.5840 的 MapReduce 与 Raft 实验；

    语言实现：Golang，使用标准库 net/rpc 替换课程中的 labrpc；

    网络模型：基于 TCP 的长连接通信模型，支持节点间 RPC 持久连接与重连策略；

核心功能
  1. Raft 一致性模块

    日志复制、选主、心跳、重试机制；

    持久化状态（term、vote、log）；

    Start() 接口供上层应用写入指令；

    applyCh 通道供状态机同步处理提交日志；

    支持 leader 崩溃后快速恢复；

  2. MapReduce 调度模块

    Master 节点将 Map / Reduce 任务写入 Raft 日志；

    Worker 向当前 Leader 发起任务申请；

    日志被 commit 后才能将任务状态更改为已分配；

    Worker 上报任务完成，Master 再次写入 Raft 更新状态；

    任务重试机制：Worker crash 或超时自动重新调度；

  3. 网络通信与容错

    使用标准库 net/rpc 完整重写通信机制；

    自定义连接池，支持 TCP 长连接复用；

    leader change 后自动重试逻辑；

    异常情况下自动重连；

亮点与扩展设计
 多 Master 选主架构
通过 Raft 模块实现 多个 Master 节点共识副本，避免传统 MapReduce 单点失效问题。

 应用层幂等机制
使用 (ClientId, RequestId) 机制防止任务重复分配与执行。

 日志级别动态调试
提供详细的日志打印，支持查看：
    leader election 过程；
    每次任务写入 Raft 日志的 Index；
    applyCh 中被提交的命令序列；
    节点之间的 RPC 请求与应答记录；

 用户可插拔的 Map/Reduce 逻辑
支持用户自定义 map/reduce 逻辑文件，只需放入 mrapps/ 并修改启动配置即可运行新任务。