package shardmap

import (
	"hash/maphash"
	"runtime"
	"sync"

	rhh "github.com/tidwall/hashmap"
)

type hasher[T comparable] func(v T) uint64

type stdHasher[T comparable] struct {
	seed maphash.Seed
}

func newStdHasher[T comparable]() stdHasher[T] {
	return stdHasher[T]{seed: maphash.MakeSeed()}
}

func (s stdHasher[T]) Hash(v T) uint64 {
	return maphash.Comparable(s.seed, v)
}

// Map is a hashmap. Like map[string]interface{}, but sharded and thread-safe.
type Map[K comparable, V any] struct {
	init   sync.Once
	cap    int
	shards int
	mus    []sync.RWMutex
	maps   []*rhh.Map[K, V]

	hasher hasher[K]

	zeroK K
	zeroV V
}

// New returns a new hashmap with the specified capacity. This function is only
// needed when you must define a minimum capacity, otherwise just use:
//
//	var m shardmap.Map
func New[K comparable, V any](cap int) *Map[K, V] {
	h := newStdHasher[K]()
	return &Map[K, V]{cap: cap, hasher: h.Hash}
}

// Clear out all values from map
func (m *Map[K, V]) Clear() {
	m.initDo()
	for i := 0; i < m.shards; i++ {
		m.mus[i].Lock()
		m.maps[i] = rhh.New[K, V](m.cap / m.shards)
		m.mus[i].Unlock()
	}
}

// Set assigns a value to a key.
// Returns the previous value, or false when no value was assigned.
func (m *Map[K, V]) Set(key K, value V) (prev any, replaced bool) {
	m.initDo()
	shard := m.choose(key)
	m.mus[shard].Lock()
	prev, replaced = m.maps[shard].Set(key, value)
	m.mus[shard].Unlock()
	return prev, replaced
}

// SetAccept assigns a value to a key. The "accept" function can be used to
// inspect the previous value, if any, and accept or reject the change.
// It's also provides a safe way to block other others from writing to the
// same shard while inspecting.
// Returns the previous value, or false when no value was assigned.
func (m *Map[K, V]) SetAccept(key K, value V, accept func(prev V, replaced bool) bool) (prev V, replaced bool) {
	m.initDo()
	shard := m.choose(key)
	m.mus[shard].Lock()
	defer m.mus[shard].Unlock()
	prev, replaced = m.maps[shard].Set(key, value)
	if accept != nil {
		if !accept(prev, replaced) {
			// revert unaccepted change
			if !replaced {
				// delete the newly set data
				m.maps[shard].Delete(key)
			} else {
				// reset updated data
				m.maps[shard].Set(key, prev)
			}
			prev, replaced = m.zeroV, false
		}
	}
	return prev, replaced
}

// Get returns a value for a key.
// Returns false when no value has been assign for key.
func (m *Map[K, V]) Get(key K) (value V, ok bool) {
	m.initDo()
	shard := m.choose(key)
	m.mus[shard].RLock()
	value, ok = m.maps[shard].Get(key)
	m.mus[shard].RUnlock()
	return value, ok
}

// Delete deletes a value for a key.
// Returns the deleted value, or false when no value was assigned.
func (m *Map[K, V]) Delete(key K) (prev V, deleted bool) {
	m.initDo()
	shard := m.choose(key)
	m.mus[shard].Lock()
	prev, deleted = m.maps[shard].Delete(key)
	m.mus[shard].Unlock()
	return prev, deleted
}

// DeleteAccept deletes a value for a key. The "accept" function can be used to
// inspect the previous value, if any, and accept or reject the change.
// It's also provides a safe way to block other others from writing to the
// same shard while inspecting.
// Returns the deleted value, or false when no value was assigned.
func (m *Map[K, V]) DeleteAccept(key K, accept func(prev V, replaced bool) bool) (prev V, deleted bool) {
	m.initDo()
	shard := m.choose(key)
	m.mus[shard].Lock()
	defer m.mus[shard].Unlock()
	prev, deleted = m.maps[shard].Delete(key)
	if accept != nil {
		if !accept(prev, deleted) {
			// revert unaccepted change
			if deleted {
				// reset updated data
				m.maps[shard].Set(key, prev)
			}
			prev, deleted = m.zeroV, false
		}
	}

	return prev, deleted
}

// Len returns the number of values in map.
func (m *Map[K, V]) Len() int {
	m.initDo()
	var len int
	for i := 0; i < m.shards; i++ {
		m.mus[i].Lock()
		len += m.maps[i].Len()
		m.mus[i].Unlock()
	}
	return len
}

// Range iterates overall all key/values.
// It's not safe to call or Set or Delete while ranging.
func (m *Map[K, V]) Range(iter func(key K, value V) bool) {
	m.initDo()
	var done bool
	for i := 0; i < m.shards; i++ {
		func() {
			m.mus[i].RLock()
			defer m.mus[i].RUnlock()
			m.maps[i].Scan(func(key K, value V) bool {
				if !iter(key, value) {
					done = true
					return false
				}
				return true
			})
		}()
		if done {
			break
		}
	}
}

func (m *Map[K, V]) choose(key K) int {
	return int(m.hasher(key) & uint64(m.shards-1))
	//return int(xxhash.Sum64String(key) & uint64(m.shards-1))
}

func (m *Map[K, V]) initDo() {
	m.init.Do(func() {
		m.shards = 1
		for m.shards < runtime.NumCPU()*16 {
			m.shards *= 2
		}
		scap := m.cap / m.shards
		m.mus = make([]sync.RWMutex, m.shards)
		m.maps = make([]*rhh.Map[K, V], m.shards)
		for i := 0; i < len(m.maps); i++ {
			m.maps[i] = rhh.New[K, V](scap)
		}
		if m.hasher == nil {
			m.hasher = newStdHasher[K]().Hash
		}
	})
}

