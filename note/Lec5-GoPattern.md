---
title: Lec5-GoPattern
date created: 2024-07-07T03:03:56.5656+08:00
date modified: 2024-12-05T01:26:59.5959+08:00
---

see [go pattern](https://pdos.csail.mit.edu/6.824/notes/gopattern.pdf)

## Goroutines for State

1. use code state to analyze the code.(e.g. \/"(\[^\\\]|\\.)\*"\/ parser)
2. covert data state to code state
3. use additional goroutine to hold additional code state
4. Think carefully before introducing unbounded queuing (**DEAD LOCK**)

## Publish or Subscribe

Key: use a goroutine to hold variable(process thread 0) along with trapping in a loop to deal with event send from struct channel map.

Instead of using mutex to lock the map(channel, bool), using **multiple levels channel** can make logic more clear.

HINT: Use goroutines to let independent concerns run independently. (Helper function)

~~~go 
func (s *Server) loop() { 
    sub := make(map[chan<- Event]bool)
    for { 
        select { 
	case e := <-s.publish:
	    for c := range sub {
	        c <- e
	    } 
	case r := <-s.subscribe: 
	    if sub[r.c] {
	        r.ok <- false
	        break 
	    }
	    sub[r.c] = true r.ok <- true
	case c := <-s.cancel:
	    if !sub[r.c] {
	        r.ok <- false 
	        break 
	    } 
	    close(r.c) 
	    delete(sub, r.c) 
	    r.ok <- true 
	}
    }
}
~~~

## Work Scheduler

1. Use channel as a block concurrent queue
2. remember that value define by for will be reuse in for-loop, so goroutine must use para to catch.(such as task:=task)
3. Hint: Know why and when each communication will proceed.(to avoid dead lock, need to deal with done channel while send task to work channel **or** just use more goroutine to independent them)
4. Hint: Make sure you know why and when each goroutine will exit

~~~go
func Schedule(servers chan string, numTask int, 
	      call func(srv string, task int) bool) { 
    work := make(chan int, numTask)
    done := make(chan bool)
    exit := make(chan bool)
    
    runTasks := func(srv string) {
        for task := range work {
            if call(srv, task) {
                done <- true
            } else {
                work <- task
            } 
        } 
    }
    // scheduler
    go func() {
	for {
	    select {
	    case srv := servers:
		go runTask(srv)
	    case <-exit: // know why and when goroutine exit
		return
	    }
	}
    }()
    
    for task := 0; task < numTask; task++ {
        work <- task
    }
    // wait for done
    for i := 0; i < numTask; i++ {
        <-done 
    } 
    close(work)
    exit <- true
}
~~~

## Replicated Server Client

1. Hint: Use a mutex if that is the clearest way to write the code.
2. Hint: Stop timers you don't need
3. Hint: Use a goto if that is the clearest way to write the code.

~~~go
func (c *Client) Call(args Args) Reply {
    type result struct {
        serverID int
        reply Reply
    }
    const timeout = 1 * time.Second
    t := time.NewTimer(timeout)
    defer t.Stop() // stop timer ...
    done := make(chan result, len(c.servers))

    c.mu.Lock() // mutex
    prefer := c.perfer
    c.mu.Unlock()

    var r result
    for off := 0; id < len(c.servers); id++ {
        id := (prefer+off) % len(c.servers)
        go func() { // independent concerns
            done <- result{id, c.callOne(c.servers[id], args)}
        }()
        select {
        case r := <-done:
            goto Done // use goto ...
        case <-t.C:
            // timeout
            t.Reset(timeout)
        }
    } 
    r := <-done
Done:
    c.mu.Lock()
    c.prefer = r.serverID
    c.mu.Unlock()
    return r.reply
}
~~~

## Protocol Multiplexer

just example

~~~go
type ProtocolMux interface { 
	// Init initializes the mux to manage messages to the given service. 
	Init(Service) 
	// Call makes a request with the given message and returns the reply. 
	// Multiple goroutines may call Call concurrently.
	Call(Msg) Msg
}
type Service interface {
	// ReadTag returns the muxing identifier in the request or reply message. 
	// Multiple goroutines may call ReadTag concurrently.
	ReadTag(Msg) int64 
	// Send sends a request message to the remote service.
	// Send must not be called concurrently with itself.
	Send(Msg)
	// Recv waits for and returns a reply message from the remote service.
	// Recv must not be called concurrently with itself. 
	Recv() Msg
}
~~~

~~~go
type Mux struct {
	srv Service
	send chan Msg
	
	mu sync.Mutex
	pending map[int64]chan<- Msg
} 
func (m *Mux) Init(srv Service) {
	m.srv = srv
	m.pending = make(map[int64]chan Msg)
	go m.sendLoop()
	go m.recvLoop() 
} 
func (m *Mux) sendLoop() {
	for args := range m.send {
		m.srv.Send(args)
	}
}
func (m *Mux) recvLoop() {
	for {
		reply := m.srv.Recv()
		tag := m.srv.ReadTag(reply)
		
		m.mu.Lock()
		done := m.pending[tag]
		delete(m.pending, tag)
		m.mu.Unlock()
		
		if done == nil {
			panic("unexpected reply")
		}
		done <- reply
	}
}
func (m *Mux) Call(args Msg) (reply Msg) {
	tag := m.srv.ReadTag(args)
	done := make(chan Msg, 1)
	
	m.mu.Lock()
	if m.pending[tag] != nil {
		m.mu.Unlock()
		panic("mux: duplicate call tag")
	}
	m.pending[tag] = done
	m.mu.Unlock()
	
	m.send <- args
	return <-done
}
~~~

## Hint

- Use the race detector, for development and even production.
- Convert data state into code state when it makes programs clearer.
- Convert mutexes into goroutines when it makes programs clearer.
- Use additional goroutines to hold additional code state.
- Use goroutines to let independent concerns run independently.
- Consider the effect of slow goroutines.
- Know why and when each communication will proceed.
- Know why and when each goroutine will exit.
- Type Ctrl-\ to kill a program and dump all its goroutine stacks.
- Use the HTTP server's /debug/pprof/goroutine to inspect live goroutine stacks.
- Use a buffered channel as a concurrent blocking queue.
- Think carefully before introducing unbounded queuing.
- Close a channel to signal that no more values will be sent.
- Stop timers you don't need.
- Prefer defer for unlocking mutexes.
- Use a mutex if that is the clearest way to write the code.
- Use a goto if that is the clearest way to write the code.
- Use goroutines, channels, and mutexes together if that is the clearest way to write the code.
