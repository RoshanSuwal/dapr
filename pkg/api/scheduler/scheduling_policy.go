package scheduler

import (
	"container/heap"
	"sync"
)

type SchedulingPolicy interface {
	Enqueue(x interface{}, priority int64)
	Dequeue() interface{}
	Name() string
	length() int
}

type FifoSchedulingPolicy struct {
	queue []interface{}
	sync  *sync.Mutex
	c     *sync.Cond
}

func (p *FifoSchedulingPolicy) Enqueue(x interface{}, priority int64) {
	p.sync.Lock()
	p.queue = append(p.queue, x)
	p.sync.Unlock()
	p.c.Signal()
}

func (p *FifoSchedulingPolicy) Dequeue() interface{} {
	p.sync.Lock()
	defer p.sync.Unlock()
	for len(p.queue) <= 0 {
		p.c.Wait()
	}

	i := p.queue[0]
	p.queue = p.queue[1:]
	return i
}

func (p *FifoSchedulingPolicy) length() int {
	return len(p.queue)
}

func (p *FifoSchedulingPolicy) Name() string {
	return "fifo"
}

func NewFifoSchedulingPolicy(queueSize int) *FifoSchedulingPolicy {
	f := &FifoSchedulingPolicy{
		queue: make([]interface{}, queueSize),
		sync:  &sync.Mutex{},
	}
	f.c = sync.NewCond(f.sync)
	return f
}

type Item struct {
	value    interface{} // The value of the item.
	priority int64       // The priority of the item.
	index    int         // The index of the item in the heap.
}

// PriorityQueue implements a priority queue based on a min-heap.
type PriorityQueue []*Item

// Len returns the length of the priority queue.
func (pq PriorityQueue) Len() int { return len(pq) }

// Less returns true if the item at index i has higher priority than the item at index j.
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

// Swap swaps the items at indices i and j.
func (pq PriorityQueue) Swap(i int, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push pushes a new item onto the priority queue.
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

// Pop pops the item with the highest priority from the priority queue.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Update modifies the priority and value of an item in the priority queue.
func (pq *PriorityQueue) Update(item *Item, value interface{}, priority int64) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

type EarliestDeadlineFirstSchedulingPolicy struct {
	queue PriorityQueue
	c     *sync.Cond
	mutex *sync.Mutex
}

func NewEarliestDeadlineFirstQueuingPolicy(queueSize int) *EarliestDeadlineFirstSchedulingPolicy {
	e := &EarliestDeadlineFirstSchedulingPolicy{
		queue: make(PriorityQueue, queueSize),
		mutex: &sync.Mutex{},
	}
	e.c = sync.NewCond(e.mutex)
	return e
}

func (p *EarliestDeadlineFirstSchedulingPolicy) Enqueue(x interface{}, priority int64) {
	p.mutex.Lock()
	heap.Push(&p.queue, &Item{value: x, priority: priority})
	p.mutex.Unlock()
	p.c.Signal()

}

func (p *EarliestDeadlineFirstSchedulingPolicy) Dequeue() interface{} {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for p.queue.Len() == 0 {
		p.c.Wait()
	}
	return heap.Pop(&p.queue).(*Item).value
}

func (p *EarliestDeadlineFirstSchedulingPolicy) length() int {
	return p.queue.Len()
}

func (p *EarliestDeadlineFirstSchedulingPolicy) Name() string {
	return "edf"
}

func NewPolicy(policyName string) SchedulingPolicy {
	switch policyName {
	case "fifo":
		return NewFifoSchedulingPolicy(0)
	case "edf":
		return NewEarliestDeadlineFirstQueuingPolicy(0)
	default:
		return NewFifoSchedulingPolicy(0)
	}
}
