package mr

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRWMap_get(t *testing.T) {
	m := RWMap{map[string]interface{}{}, &sync.RWMutex{}}
	m.put("k1", 12)
	go func() {
		for i := 0; i < 100; i++ {
			val, _ := m.get("k1")
			fmt.Printf("%v \n", val)
		}
	}()
	for i := 0; i < 100; i++ {
		m.put("k1", i)
		time.Sleep(1)
	}
	time.Sleep(1000)
}
