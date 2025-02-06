---
title: Pow
date created: 2024-09-27T14:58:42.4242+08:00
date modified: 2024-12-05T01:26:27.2727+08:00
---

Pow 的共识的主要实现是通过限制每次交易块产生的成本（算力）来保证对一个货币的操作是不可重复的。

## 过程

发起交易者声明一个交易，声明一个新的区块。

各个 Worker 在一个区块中完成 Work 中验证，完成验证者更新并将凭证和新的区块扩散到所有矿工。

## Work

trying many different nonces until one yields a block hash with enough leading zero bits.

一个极为困难的任务，大约 10 分钟才可以完成一个交易块的确认。

## 51% 可信节点

只有达成 51% 的算力，才能够在新的分叉上不断扩展，超过旧有的长链，同时覆盖过去的交易，达到双花的效果。
