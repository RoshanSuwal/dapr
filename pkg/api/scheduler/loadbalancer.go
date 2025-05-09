package scheduler

import (
	nr "github.com/dapr/components-contrib/nameresolution"
	"math"
	"sync"
)

type LoadBalancer interface {
	Type() string
	Select(addressList nr.AddressList) string
	AddServer(address string)
	UpdateActiveConnections(address string, increment int)
}

type RoundRobinLoadBalancer struct {
}

func (l *RoundRobinLoadBalancer) Type() string { return "Round Robin" }

func (l *RoundRobinLoadBalancer) Select(addressList nr.AddressList) string { return addressList.Pick() }

func (l *RoundRobinLoadBalancer) AddServer(address string) {}

func (l *RoundRobinLoadBalancer) UpdateActiveConnections(address string, increment int) {}

type LeastConnectionLoadBalancer struct {
	AddressMap map[string]int
	mu         sync.Mutex
}

func (l *LeastConnectionLoadBalancer) Type() string { return "Least Connection" }

func (l *LeastConnectionLoadBalancer) Select(addressList nr.AddressList) (address string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	minConn := math.MaxInt
	for i := range len(addressList) {
		if _, ok := l.AddressMap[addressList[i]]; !ok {
			l.AddressMap[addressList[i]] = 0
		}

		if l.AddressMap[addressList[i]] < minConn {
			minConn = l.AddressMap[addressList[i]]
			address = addressList[i]
		}
	}
	return address
}

func (l *LeastConnectionLoadBalancer) UpdateActiveConnections(address string, increment int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if v, ok := l.AddressMap[address]; ok {
		l.AddressMap[address] = max(0, v+increment)
	}
}

func (l *LeastConnectionLoadBalancer) AddServer(address string) {}

func NewLoadBalancer(lbType string) LoadBalancer {
	switch lbType {
	case "round_robin":
		return &RoundRobinLoadBalancer{}
	case "least_connection":
		return &LeastConnectionLoadBalancer{AddressMap: make(map[string]int)}
	default:
		return &RoundRobinLoadBalancer{}
	}
}
