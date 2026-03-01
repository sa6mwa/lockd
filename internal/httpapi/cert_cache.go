package httpapi

import (
	"container/list"
	"crypto/sha256"
	"crypto/x509"
	"sync"
)

const (
	defaultClientIdentityCacheLimit  = 4096
	defaultNamespaceClaimsCacheLimit = 4096
)

type certCacheKey [sha256.Size]byte

func certificateCacheKey(cert *x509.Certificate) (certCacheKey, bool) {
	if cert == nil {
		return certCacheKey{}, false
	}
	if len(cert.Raw) > 0 {
		return sha256.Sum256(cert.Raw), true
	}
	serial := ""
	if cert.SerialNumber != nil {
		serial = cert.SerialNumber.Text(16)
	}
	h := sha256.New()
	_, _ = h.Write([]byte(cert.Subject.String()))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(cert.Issuer.String()))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(serial))
	_, _ = h.Write([]byte{0})
	for _, uri := range cert.URIs {
		if uri == nil {
			continue
		}
		_, _ = h.Write([]byte(uri.String()))
		_, _ = h.Write([]byte{0})
	}
	for _, dns := range cert.DNSNames {
		_, _ = h.Write([]byte(dns))
		_, _ = h.Write([]byte{0})
	}
	for _, email := range cert.EmailAddresses {
		_, _ = h.Write([]byte(email))
		_, _ = h.Write([]byte{0})
	}
	for _, ip := range cert.IPAddresses {
		_, _ = h.Write([]byte(ip.String()))
		_, _ = h.Write([]byte{0})
	}
	var key certCacheKey
	copy(key[:], h.Sum(nil))
	return key, true
}

type lruCache[K comparable, V any] struct {
	mu    sync.Mutex
	max   int
	items map[K]*list.Element
	order *list.List
}

type lruCacheEntry[K comparable, V any] struct {
	key   K
	value V
}

func newLRUCache[K comparable, V any](max int) *lruCache[K, V] {
	if max <= 0 {
		return nil
	}
	return &lruCache[K, V]{
		max:   max,
		items: make(map[K]*list.Element, max),
		order: list.New(),
	}
}

func (c *lruCache[K, V]) get(key K) (V, bool) {
	var zero V
	if c == nil {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return zero, false
	}
	c.order.MoveToFront(elem)
	return elem.Value.(*lruCacheEntry[K, V]).value, true
}

func (c *lruCache[K, V]) put(key K, value V) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*lruCacheEntry[K, V])
		entry.value = value
		c.order.MoveToFront(elem)
		return
	}
	elem := c.order.PushFront(&lruCacheEntry[K, V]{key: key, value: value})
	c.items[key] = elem
	if c.order.Len() <= c.max {
		return
	}
	back := c.order.Back()
	if back == nil {
		return
	}
	entry := back.Value.(*lruCacheEntry[K, V])
	delete(c.items, entry.key)
	c.order.Remove(back)
}

func (c *lruCache[K, V]) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.items)
	c.order.Init()
}

func (c *lruCache[K, V]) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
