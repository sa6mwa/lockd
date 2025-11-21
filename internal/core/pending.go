package core

import (
	"strings"
	"sync"
)

// PendingDeliveries tracks in-flight deliveries per namespace/queue/owner so transports
// can release or clear them on disconnect.
type PendingDeliveries struct {
	mu     sync.Mutex
	owners map[string]map[string]map[string]*QueueDelivery // key -> owner -> msgID -> delivery
}

// NewPendingDeliveries constructs a pending delivery tracker keyed by namespace/queue/owner.
func NewPendingDeliveries() *PendingDeliveries {
	return &PendingDeliveries{
		owners: make(map[string]map[string]map[string]*QueueDelivery),
	}
}

func pendingKey(namespace, queue string) string {
	namespace = strings.TrimSpace(namespace)
	queue = strings.TrimSpace(queue)
	if namespace == "" {
		return queue
	}
	if queue == "" {
		return namespace
	}
	return namespace + "/" + queue
}

// Track registers a delivery for an owner.
func (p *PendingDeliveries) Track(namespace, queue, owner string, d *QueueDelivery) {
	if p == nil || d == nil || d.Message == nil || owner == "" {
		return
	}
	key := pendingKey(namespace, queue)
	p.mu.Lock()
	defer p.mu.Unlock()
	owners := p.owners[key]
	if owners == nil {
		owners = make(map[string]map[string]*QueueDelivery)
		p.owners[key] = owners
	}
	msgs := owners[owner]
	if msgs == nil {
		msgs = make(map[string]*QueueDelivery)
		owners[owner] = msgs
	}
	msgs[d.Message.MessageID] = d
}

// Clear removes a tracked delivery.
func (p *PendingDeliveries) Clear(namespace, queue, owner, messageID string) {
	if p == nil || owner == "" || messageID == "" {
		return
	}
	key := pendingKey(namespace, queue)
	p.mu.Lock()
	defer p.mu.Unlock()
	owners := p.owners[key]
	if owners == nil {
		return
	}
	if msgs, ok := owners[owner]; ok {
		delete(msgs, messageID)
		if len(msgs) == 0 {
			delete(owners, owner)
		}
	}
	if len(owners) == 0 {
		delete(p.owners, key)
	}
}

// Release aborts all deliveries for an owner and removes the entry.
func (p *PendingDeliveries) Release(namespace, queue, owner string) {
	if p == nil || owner == "" {
		return
	}
	key := pendingKey(namespace, queue)
	p.mu.Lock()
	owners := p.owners[key]
	if owners == nil {
		p.mu.Unlock()
		return
	}
	msgs := owners[owner]
	delete(owners, owner)
	if len(owners) == 0 {
		delete(p.owners, key)
	}
	p.mu.Unlock()
	for _, d := range msgs {
		if d != nil && d.Finalize != nil {
			d.Finalize(false)
		}
	}
}
