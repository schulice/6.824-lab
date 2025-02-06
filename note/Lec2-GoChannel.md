---
title: Lec2-GoChannel
date created: 2024-07-05T14:39:23.2323+08:00
date modified: 2024-12-05T01:27:16.1616+08:00
---

## Channel

1. synchronous
2. two things: communication, notification

## 2 Kind of Concurrency

1. status - sharing data and locks
2. communication - channels (or cond)

## 3 Different Kind Crawl

### Serial

keep a dfa map

## Concurrent with Lock

mutex lock the share data and do fetch and change map one time

### Concurrent with Channel

use channel to make each worker deliver the fetched url to channel while coordinator select one from channel then update and send it to new worker. Channel do the lock do but on communication method
