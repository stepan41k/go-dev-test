package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*Queue),
	}
}

type Queue struct {
	mu       sync.Mutex
	messages []string
	waiters  []chan string
}

func NewQueue() *Queue {
	return &Queue{
		messages: make([]string, 0),
		waiters:  make([]chan string, 0),
	}
}

func main() {
	port := flag.String("port", "8080", "port to listen on")
	flag.Parse()

	broker := NewBroker()

	http.HandleFunc("/", broker.handleRequest)

	fmt.Printf("server started on :%s\n", *port)

	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		fmt.Printf("server starting error: %s\n", err)
	}
}

func (b *Broker) handleRequest(w http.ResponseWriter, r *http.Request) {
	queueName := strings.TrimPrefix(r.URL.Path, "/")
	if queueName == "" {
		http.NotFound(w, r)
		return
	}

	b.mu.RLock()
	q, ok := b.queues[queueName]
	b.mu.RUnlock()
	if !ok {
		b.mu.Lock()
		q, ok = b.queues[queueName]
		if !ok {
			q = NewQueue()
			b.queues[queueName] = q
		}
		b.mu.Unlock()
	}

	switch r.Method {
	case http.MethodPut:
		val := r.URL.Query().Get("v")
		if val == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		q.push(val)

	case http.MethodGet:
		timeoutStr := r.URL.Query().Get("timeout")
		timeout := 0
		if timeoutStr != "" {
			timeout, _ = strconv.Atoi(timeoutStr)
		}

		msg, ok := q.pop(time.Duration(timeout) * time.Second)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Write([]byte(msg))

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (q *Queue) push(msg string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.waiters) > 0 {
		waiter := q.waiters[0]
		q.waiters = q.waiters[1:]
		waiter <- msg
		return
	}

	q.messages = append(q.messages, msg)
}

func (q *Queue) pop(timeout time.Duration) (string, bool) {
	q.mu.Lock()

	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages[0] = ""
		q.messages = q.messages[1:]
		q.mu.Unlock()
		return msg, true
	}

	if timeout <= 0 {
		q.mu.Unlock()
		return "", false
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ch := make(chan string, 1)
	q.waiters = append(q.waiters, ch)
	q.mu.Unlock()

	select {
	case msg := <-ch:
		return msg, true
	case <-timer.C:
		q.mu.Lock()
		defer q.mu.Unlock()
		for i, w := range q.waiters {
			if w == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		return "", false
	}
}