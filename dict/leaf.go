package dict

import (
	"strings"
	"sync"
)

type LeafDictonary struct {
	sync.RWMutex
	mm map[string]uint32
	ms []string
}

func NewDictonary(c int) *LeafDictonary {
	return &LeafDictonary{
		mm: make(map[string]uint32, c),
		ms: make([]string, 0, c),
	}
}

func (ld *LeafDictonary) Length() int {
	ld.RLock()
	l := len(ld.ms)
	ld.RUnlock()
	return l
}

func (ld *LeafDictonary) Put(b string) uint32 {
	ld.Lock()
	if i, ok := ld.mm[b]; ok {
		ld.Unlock()
		return i
	}
	i := len(ld.ms)
	ld.ms = append(ld.ms, b)
	ld.mm[b] = uint32(i)
	ld.Unlock()
	return uint32(i)
}

func (ld *LeafDictonary) In(b string) (uint32, bool) {
	ld.RLock()
	if i, ok := ld.mm[b]; ok {
		ld.RUnlock()
		return i, true
	}
	ld.RUnlock()
	return 0, false
}

func (ld *LeafDictonary) Get(n uint32) string {
	ld.RLock()
	if int(n) < len(ld.ms) {
		ld.RUnlock()
		return ld.ms[int(n)]
	}
	ld.RUnlock()
	return ""
}

func (ld *LeafDictonary) Compare(x, y uint32) int {
	return strings.Compare(ld.Get(x), ld.Get(y))
}
