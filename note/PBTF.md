---
title: PBTF
date created: 2024-09-27T14:03:39.3939+08:00
date modified: 2024-12-05T01:26:19.1919+08:00
---

## 参与的成员

1. client: 通过 clerk 与所有 replicas 交互
2. leader(primary): replicas 的主服务器
3. replica(peer): 服务器

## PBFT 希望解决的问题

在 Server 总节点数量至少为 3f+1，存在 f 个不可信节点（该节点会恶意返回错误的内容来破坏整个系统）的情况下，存在网络分区等情况下，仍然可以保证正确的一致性。

## 操作流程

### 提交 TXN 过程

```
- client
向所有peer发送txn。
接受到f+1个回复则说明txn成功执行。

- primary
在接受到的txn中，选择一个txn及其操作序号n，向所有peer发送`PRE-PREPARE(txn, n)`。

- replica
接受到`PRE-PREPAR`E后，向所有的peer发送对应的`PREPARE`的msg。
仅当接收到`n==next`的`PREPARE`的计数大于2f+1时，认为该txn成功扩散，向所有节点发送`COMIMT(txn, n)`。
仅当接受到`n==next`的`COMMIT`计数大于2f+1时，认为所有replica都成功的确认了该txn，于是执行txn，并向client回复。

```

### 更改 Primary

```
- replica
等待COMMIT超时，向新的Primary发送VIEW-CHANGE消息
(新的primary的可能为`n%peers`)
(VIEW-CHANGE msg中包含各个序号的状态->[prepared, committed]的(TXN, n))

- new primary
等待2f+1的VIEW-CHANGE消息
进入FIX-LOG过程，确认消息序号的状态(PREPARED|COMITTED)，确认下一序号（应该修复的操作)
向所有节点发送NEW-VIEW信息

```

## 为何需要 2f+1 的可信节点

为了使得在含有 f 个不可信节点构成的每次提交确认的 2f+1 中，仍然可以覆盖大多数的可信节点，使得可信节点始终在确认中处于多数地位，正确的操作序列可以通过多数好节点在多个共识轮次中扩散。

## 是否存在无法进行的状态

不会，最坏的情况是延迟。
