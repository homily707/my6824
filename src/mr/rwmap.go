package mr

import "sync"

type RWMap struct {
	m map[string]interface{}
	*sync.RWMutex
}

func NewRWMap() RWMap {
	return RWMap{
		m:       map[string]interface{}{},
		RWMutex: &sync.RWMutex{},
	}
}

func (rw RWMap) get(key string) (interface{}, bool) {
	rw.RLock()
	defer rw.RUnlock()
	val, ok := rw.m[key]
	return val, ok
}

func (rw *RWMap) put(key string, value interface{}) {
	rw.Lock()
	defer rw.Unlock()
	rw.m[key] = value
}
