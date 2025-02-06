---
title: Lec10-Two-Phase Commit
date: 2024-08-04T23:38:19+08:00
aliases: []
tags: []
---

# Two-Phase Locking

Locks -> Lock point -> Unlocks

2PL: expanding -> shrinking, can unlock the no use lock earlier, but cause dirt read.

strict 2PL: only shrinking after finishing all operation

# Transaction

## ACID

- Atomic -- all write or none
- Consistent -- application specified invariant
- Isolated -- no interference between xactions - serializability
- Duration -- persist

## Implement

### Concurrency Control

- pessimistic
- optimistic

### Failures

suppose atomic commit

# 2PC

TC, A, B

```
A, B lock
TC -> B: Preperation
TC <- B: YES / NO (B persist)
TC -> B: COMMIT(all respond YES)
A, B verify txn
A, B unlock
```

1. B Crash
	1. before got PREPERATION, forgot
	2. after send YES, MUST remember
	3. above, but no meet COMMIT, ask the TC
2. TC Crash
	1. after send COMMIT, MUST remember
	2. not receive YES / NO, time out
3. Forget txn
	1. TC: see all YES, and commit
	2. B: after TC COMMIT, get another COMMIT

Usage: sharded DBs, single small domain

# Difference from Raft

- 2PC is used to perform the txn on a shard service. (ALL)
- Raft do the same thing between servers. (Majority)
