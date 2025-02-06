---
title: Lec4-Linearizability
date created: 2024-07-05T16:27:53.5353+08:00
date modified: 2024-12-05T01:27:11.1111+08:00
---

## Linearizability

~~~
C1: |--------Wx1---------| (due to retransmission)
C2:        |-Wx2-|
C3:   |-Rx1-| |-Rx2-|  |-Rx1-|
~~~

we can try to select some time point from invocation and respond for each operation and so much serial execution are same as history record

## Write and Read

1. write is server according
2. read - client
3. all client share a same write order

## Feature(from text)

  * reads see fresh data -- not stale
  * all clients see the same data (when there aren't writes)
  * all clients see data changes in the same order

## Strong or Eventual Consistency

* a general pattern: choose one between (strong consistency) and (maximize availability)
* strong: poor availability (wait more)
* eventual: poor consistency

## METERIAL

https://anishathalye.com/testing-distributed-systems-for-linearizability/
