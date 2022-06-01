package mr

import "sync"

type RWMap struct {
	m map[string]Job
	*sync.RWMutex
}

func NewRWMap() RWMap {
	return RWMap{
		m:       map[string]Job{},
		RWMutex: &sync.RWMutex{},
	}
}

func (rw RWMap) get(key string) (Job, bool) {
	rw.RLock()
	defer rw.RUnlock()
	val, ok := rw.m[key]
	return val, ok
}

func (rw *RWMap) put(key string, value Job) {
	rw.Lock()
	defer rw.Unlock()
	rw.m[key] = value
}

func (rw *RWMap) clear() {
	rw.Lock()
	defer rw.Unlock()
	for k := range rw.m {
		delete(rw.m, k)
	}
}
