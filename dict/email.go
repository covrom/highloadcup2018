package dict

import (
	"sort"
	"strings"
	"sync"
)

var DictonaryEml = NewEmailDictonary(1600000)

// корень из 2 букв, выборка по 2 и 3 буквы email

type emlRec struct {
	email string
	id    uint32
}

type EmailDictonary struct {
	sync.RWMutex
	// сортированные в лексикографическом порядке
	order []uint16
	radix map[uint16][]emlRec
	emls  []string // индекс = id
}

func NewEmailDictonary(c int) *EmailDictonary {
	return &EmailDictonary{
		order: make([]uint16, 0, 256),
		radix: make(map[uint16][]emlRec, 256),
		emls:  make([]string, c),
	}
}

func (ld *EmailDictonary) Put(id uint32, b string) {
	if len(b) < 2 {
		return
	}
	ld.Lock()

	for len(ld.emls) <= int(id) {
		ld.emls = append(ld.emls, "")
	}

	ld.emls[int(id)] = b

	root := uint16(b[0])<<8 | uint16(b[1])

	lo := len(ld.order)
	ri := sort.Search(lo, func(i int) bool {
		return ld.order[i] >= root
	})
	if ri == lo {
		ld.order = append(ld.order, root)
	} else if ld.order[ri] != root {
		ld.order = append(ld.order, root)
		copy(ld.order[ri+1:], ld.order[ri:])
		ld.order[ri] = root
	}

	recs := ld.radix[root]
	ln := len(recs)
	idx := sort.Search(ln, func(i int) bool {
		return strings.Compare(recs[i].email, b) >= 0
	})
	if idx < ln {
		if recs[idx].email == b {
			recs[idx].id = id
		} else {
			recs = append(recs, emlRec{})
			copy(recs[idx+1:], recs[idx:])
			recs[idx] = emlRec{b, id}
		}
	} else {
		recs = append(recs, emlRec{b, id})
	}
	ld.radix[root] = recs
	ld.Unlock()
}

func (ld *EmailDictonary) In(b string) (uint32, bool) {
	if len(b) < 2 {
		return 0, false
	}
	ld.RLock()
	root := uint16(b[0])<<8 | uint16(b[1])
	recs := ld.radix[root]
	ln := len(recs)
	idx := sort.Search(ln, func(i int) bool {
		return strings.Compare(recs[i].email, b) >= 0
	})
	if idx < ln && recs[idx].email == b {
		ret := recs[idx].id
		ld.RUnlock()
		return ret, true
	}
	ld.RUnlock()
	return 0, false
}

func (ld *EmailDictonary) Get(id uint32) string {
	ld.RLock()
	if int(id) < len(ld.emls) {
		ld.RUnlock()
		return ld.emls[int(id)]
	}
	ld.RUnlock()
	return ""
}

func (ld *EmailDictonary) Compare(x, y uint32) int {
	return strings.Compare(ld.Get(x), ld.Get(y))
}

func (ld *EmailDictonary) Gt(pfx string, f func(id uint32)) {
	if len(pfx) == 1 {
		pfx += " "
	}
	root := uint16(pfx[0])<<8 | uint16(pfx[1])

	ld.RLock()

	lo := len(ld.order)
	ri := sort.Search(lo, func(i int) bool {
		return ld.order[i] >= root
	})
	fsrch := true
	for ri < lo {
		recs := ld.radix[ld.order[ri]]
		if fsrch {
			ln := len(recs)
			idx := sort.Search(ln, func(i int) bool {
				return strings.Compare(recs[i].email, pfx) >= 0
			})
			for idx < ln {
				f(recs[idx].id)
				idx++
			}
			fsrch = false
		} else {
			for _, rec := range recs {
				f(rec.id)
			}
		}
		ri++
	}
	ld.RUnlock()
}

func (ld *EmailDictonary) Lt(pfx string, f func(id uint32)) {
	if len(pfx) == 1 {
		pfx += " "
	}
	root := uint16(pfx[0])<<8 | uint16(pfx[1])

	ld.RLock()

	lo := len(ld.order)
	ri := sort.Search(lo, func(i int) bool {
		return ld.order[i] >= root
	})
	fsrch := true
	for ri < lo && ri >= 0 {
		recs := ld.radix[ld.order[ri]]
		if fsrch {
			ln := len(recs)
			idx := sort.Search(ln, func(i int) bool {
				return strings.Compare(recs[i].email, pfx) >= 0
			})
			for idx < ln && idx >= 0 {
				f(recs[idx].id)
				idx--
			}
			fsrch = false
		} else {
			for _, rec := range recs {
				f(rec.id)
			}
		}
		ri--
	}
	ld.RUnlock()
}
