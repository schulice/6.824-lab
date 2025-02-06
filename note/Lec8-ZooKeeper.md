---
title: Lec8-ZooKeeper
date: 2024-07-27T00:45:29+08:00
aliases: []
tags: []
---

# Introduction

ZooKeeper is a coordinate kernel which exporting an API that enable application to develop their own primitive. Instead of using lock, ZooKeeper hence implements the API that **manipulates simple waitfree data objects organized hierarchically as in file systems.**

1. A simple pipeline architecture to have outstanding performace
2. Zab, a leader-base atomic broadcast protocol, but not use on local reading op order
3. A watch mechanism. Client use to to watch for the update to data, and get the notification upon an update

# Service

## Overview

Node path is like the filesystem on unix-like system

Znode has 2 kinds:

- Regular
- Ephemeral: znode can be deleted explicitly or be remove by system automatically.

Znode also provide a *sequential* znode witch find a large enough n to create filename_n under the directory entry.

Watches are one-times triggers associated with session and unregistered after notificate GET data is changed or session is closed.

### Data Model

Znode map to abstraction of the client application, typically corresponding the metadata for coordination purposes.

Still ZooKeeper allow clients to store some information for metadata or configuration.Znode also have associated meta-data with time stamp and version count.

### Session

A client connect to ZooKeeper and init a session. Zookeeper use Session to keep the trace of a client.(still time out checker)

## API

- Create(path, data, flag)
- Delete(path, verision)
- Exists(path, watch): wath flag enable client watch to this znode
- GetData(path, watch)
- SetData(path, data, version): Set data when version == currentVersion
- GetChildren(path, watch)
- Sync(path): wait for all update (client) to propagate to the server. The path currently is ignored.

Each API have synchronized and asynchronized version. ZooKeeper guarantee the correct order of callbacks.

Znode do not have a handle which simply the state.

If version set to -1, not check version.

## Guarantees

- Linearizable writes: update requests are serializable and respect precedence.
- FIFO client order: execute in order
- Linearizable for write and FIFO for read.

Linearizabllity different from Herlihy's which a client only able to have one outstanding operation at a time, ZooKeeper's allow client to have multiple outstanding operations and still guarantee the FIFO order.

Read the leader scenario on paper.

## Primitives Example

- Configuration Management: like seqlock in linux kernel, but use watch to identify the data whether be changed, if so, retry.
- Rendezvous: master detect the zr file(worker specified) to get the actual information of worker after it start.
- Group Membership: ephemeral znode under group path, autoremove when client offline.
- Lock:
  - Simple lock: Use lock file, only get the lock when no lock file then creat it, then unlock by deleting it. BUT is will meet herd effect. (Everyone know file was removed).
  - Simple lock without herd effect: Use seqential file, each lock operation create th sequential file then check self is the least one to get lock, else wait the before less one removed.(Easy to debug lock)
  - RW lock: read-n and write-n under the directory, if no exit the lower write node, the read operation can process else wait the before write lock. Write lock is as same as mutex.
- Double Barrier: Use a limit number children under a directory. Process check exit condition after a particular child to disappear.

# Application

PASS, see paper

# Implementation

ZooKeeper also use a leader follower structure to apply the proposals.

## Request Processor

Network layer is actomic.

Transactions are idempotent.

Use setDataTXN and errorTXN to handle the future operation by client.

## Atomic Boardcast

Some feature similar to raft.

Use TCP to send ordered.

But do not persistent the index of every delivered message.

## Replicate Database

No lock. Due to the idempotent transactions, just need to reimplement the transcations to snapshot can get the same state even if change happened on creating replica.

So, the snapshot of ZooKeeper is fuzzy.

## Client-Server Interactions

Server process write request in order, and send notification to all linked client(locally).

Each read request process on local server with the zxid tag which provide partical order.

This kind of read cause no read value writed immediately, but ZooKeeper provide a primitive that just read after a sync request. In implement, this sync request only put on the last of request queue.

Implement the leader timeout to avoid send empty transaction to follower pending queue to avoid wasting of network.

Client wil not connect to the server with the less last zxid.

Let client to send the heartbeat to server when less operation. If timeout, change to another server.
