package ssdb

import (
	"sync"
)

type rcmutex struct {
	rc int64
	mu sync.Mutex
}

type KeyMutex struct {
	mu   sync.Mutex
	keys map[string]*rcmutex
}

func NewKeyMutex() *KeyMutex {
	return &KeyMutex{
		keys: make(map[string]*rcmutex),
	}
}

func (km *KeyMutex) Lock(key string) {
	km.mu.Lock()
	rcmu, ok := km.keys[key]
	if !ok {
		rcmu = new(rcmutex)
		km.keys[key] = rcmu
	}
	rcmu.rc++
	km.mu.Unlock()

	rcmu.mu.Lock()
}

func (km *KeyMutex) Unlock(key string) {
	km.mu.Lock()
	rcmu, ok := km.keys[key]
	if !ok {
		km.mu.Unlock()
		return
	}
	rcmu.rc--
	if rcmu.rc == 0 {
		delete(km.keys, key)
	}
	km.mu.Unlock()

	rcmu.mu.Unlock()
}
