package db

import (
	"sort"
)

type IDIterator interface {
	HasNext() bool
	NextID() IDAcc
	JumpTo(IDAcc) bool // результат как у HasNext
	Range() (IDAcc, IDAcc)
	Cardinality() int32
	Reversed() bool
	Clone() IDIterator
}

func (c *Column) Iterator(reverse bool, useFilter bool, filterVal DataEntry, filterNEQ bool) *ColumnIterator {
	if reverse {
		return &ColumnIterator{
			pos:       int32(c.maxId) + 1,
			grow:      -1,
			col:       c,
			use1b:     c.use1b,
			use2b:     c.use2b,
			maxpos:    int32(c.maxId),
			minpos:    int32(c.minId),
			useFilter: useFilter,
			filterVal: filterVal,
			filterNEQ: filterNEQ,
		}
	} else {
		return &ColumnIterator{
			pos:       int32(c.minId) - 1,
			grow:      1,
			col:       c,
			use1b:     c.use1b,
			use2b:     c.use2b,
			maxpos:    int32(c.maxId),
			minpos:    int32(c.minId),
			useFilter: useFilter,
			filterVal: filterVal,
			filterNEQ: filterNEQ,
		}
	}
}

// filter должен быть отсортирован по возрастанию
func (c *Column) IteratorWithFilterId(filter []IDAcc, reverse bool) IDIterator {
	if reverse {
		return &RangeIterator{
			pos:    int32(len(filter)),
			grow:   -1,
			col:    c,
			maxpos: int32(len(filter)) - 1,
			minpos: 0,
			filter: filter,
		}
	} else {
		return &RangeIterator{
			pos:    -1,
			grow:   1,
			col:    c,
			maxpos: int32(len(filter)) - 1,
			minpos: 0,
			filter: filter,
		}
	}
}

func IteratorByIds(filter []IDAcc, reverse bool) IDIterator {
	if reverse {
		return &RangeIterator{
			pos:    int32(len(filter)),
			grow:   -1,
			col:    nil,
			maxpos: int32(len(filter)) - 1,
			minpos: 0,
			filter: filter,
		}
	} else {
		return &RangeIterator{
			pos:    -1,
			grow:   1,
			col:    nil,
			maxpos: int32(len(filter)) - 1,
			minpos: 0,
			filter: filter,
		}
	}
}

func (c *Column) IteratorWithFilterVal(filter DataEntry, reverse, noneq bool) (ret IDIterator) {
	if c.use1b || c.use2b || c.use4b || noneq {
		ret = c.Iterator(reverse, true, filter, noneq)
	} else {
		ids := c.GetV(filter)
		ret = c.IteratorWithFilterId(ids, reverse)
	}

	return ret
}

type ColumnIterator struct {
	pos        int32
	grow       int32
	maxpos     int32
	minpos     int32
	col        *Column
	use1b      bool
	use2b      bool
	useFilter  bool
	filterVal  DataEntry
	filterNEQ  bool
	lastJumpTo IDAcc
	lastJumpOk bool
}

func (iter *ColumnIterator) Clone() IDIterator {
	rv := &ColumnIterator{}
	*rv = *iter
	return rv
}

func (iter *ColumnIterator) Cardinality() int32 {
	return iter.maxpos - iter.minpos + 1
}

func (iter *ColumnIterator) Reversed() bool {
	return iter.grow < 0
}

func (iter *ColumnIterator) Range() (IDAcc, IDAcc) {
	a, b := IDAcc(iter.minpos), IDAcc(iter.maxpos)
	if a > b {
		a, b = b, a
	}
	return a, b
}

func (iter *ColumnIterator) JumpTo(id IDAcc) bool {
	if iter.lastJumpTo == id {
		return iter.lastJumpOk
	}
	iter.lastJumpTo = id
	newpos := int32(id)
	if newpos < iter.minpos || newpos > iter.maxpos {
		iter.lastJumpOk = false
		return false
	}
	if iter.pos == newpos {
		iter.lastJumpOk = true
		return true
	}
	iter.pos = newpos - iter.grow
	iter.lastJumpOk = iter.HasNext()
	return iter.lastJumpOk
}

func (iter *ColumnIterator) HasNext() bool {
	ipos, igrow, imin, imax, ifv, ifneq, iempty := iter.pos, iter.grow, iter.minpos, iter.maxpos, iter.filterVal, iter.filterNEQ, iter.col.empty
	ipos += igrow

	if ipos >= imin && ipos <= imax {
		if iter.useFilter {
			if iter.use1b {
			lp:
				for {
					pos, sub := ipos>>6, uint32(ipos)&0x3f
					vv := iter.col.bmp[pos]
					mask := uint64(1) << sub
					cmpv := uint64(ifv) << sub
					if igrow > 0 {
						cnt := 0x40 - sub
						for cnt > 0 {
							if (ifneq && cmpv != vv&mask) ||
								(!ifneq && cmpv == vv&mask) {
								break lp
							}
							mask = mask << 1
							cmpv = cmpv << 1
							cnt--
							ipos++
							if ipos < imin || ipos > imax {
								break lp
							}
						}
					} else {
						cnt := sub + 1
						for cnt > 0 {
							if (ifneq && cmpv != vv&mask) ||
								(!ifneq && cmpv == vv&mask) {
								break lp
							}
							mask = mask >> 1
							cmpv = cmpv >> 1
							cnt--
							ipos--
							if ipos < imin || ipos > imax {
								break lp
							}
						}
					}
				}
			} else if iter.use2b {
			lp2:
				for {
					pos, sub := ipos>>5, uint32(ipos)&0x1f
					vv := iter.col.bmp[pos]
					mask := uint64(3) << (sub * 2)
					cmpv := uint64(ifv) << (sub * 2)
					if igrow > 0 {
						cnt := 0x20 - sub
						for cnt > 0 {
							if (ifneq && cmpv != vv&mask) ||
								(!ifneq && cmpv == vv&mask) {
								break lp2
							}
							mask = mask << 2
							cmpv = cmpv << 2
							cnt--
							ipos++
							if ipos < imin || ipos > imax {
								break lp2
							}
						}
					} else {
						cnt := sub + 1
						for cnt > 0 {
							if (ifneq && cmpv != vv&mask) ||
								(!ifneq && cmpv == vv&mask) {
								break lp2
							}
							mask = mask >> 2
							cmpv = cmpv >> 2
							cnt--
							ipos--
							if ipos < imin || ipos > imax {
								break lp2
							}
						}
					}
				}
			} else {
				for {
					v := iter.col.Get(IDAcc(ipos))
					if ifneq && v != ifv && v != NullEntry && v != iempty {
						break
					}
					if !ifneq && v == ifv {
						break
					}
					ipos += igrow
					if ipos < imin || ipos > imax {
						break
					}
				}
			}
		} else {
			if iter.use1b {
				for !SmAcc.Contains(IDAcc(ipos)) {
					ipos += igrow
					if ipos < imin || ipos > imax {
						break
					}
				}
			} else {
				for !iter.col.Contains(IDAcc(ipos)) {
					ipos += igrow
					if ipos < imin || ipos > imax {
						break
					}
				}
			}
		}
	}
	iter.pos = ipos
	return ipos >= imin && ipos <= imax
}

func (iter *ColumnIterator) NextID() IDAcc {
	return IDAcc(iter.pos)
}

type RangeIterator struct {
	pos        int32
	grow       int32
	maxpos     int32
	minpos     int32
	col        *Column
	filter     []IDAcc
	lastJumpTo IDAcc
	lastJumpOk bool
}

func (iter *RangeIterator) Clone() IDIterator {
	rv := &RangeIterator{}
	*rv = *iter
	return rv
}

func (iter *RangeIterator) Cardinality() int32 {
	return iter.maxpos - iter.minpos + 1
}

func (iter *RangeIterator) Reversed() bool {
	return iter.grow < 0
}

func (iter *RangeIterator) Range() (IDAcc, IDAcc) {
	if iter.maxpos < 0 {
		return 0, 0
	}
	a, b := iter.filter[iter.minpos], iter.filter[iter.maxpos]
	if a > b {
		a, b = b, a
	}
	return a, b
}

func (iter *RangeIterator) JumpTo(id IDAcc) bool {
	if iter.maxpos < 0 {
		return false
	}
	if iter.lastJumpTo == id {
		return iter.lastJumpOk
	}
	iter.lastJumpTo = id

	filter := iter.filter
	delta := int32(0)
	if iter.pos >= iter.minpos && iter.pos <= iter.maxpos {
		if filter[iter.pos] < id {
			filter = filter[iter.pos:]
			delta = iter.pos
		} else if filter[iter.pos] > id {
			filter = filter[:iter.pos+1]
		} else {
			iter.lastJumpOk = true
			return true
		}
	}

	ln := len(filter)
	// проверим границы
	if ln == 0 {
		iter.lastJumpOk = false
		return false
	}
	if (filter[0] > id && iter.grow < 0) || (filter[ln-1] < id && iter.grow > 0) {
		iter.lastJumpOk = false
		return false
	}

	n := uint32(len(filter))
	i, j := uint32(0), n

	if n > 96 && id >= filter[0] && id <= filter[n-1] {
		// jump-проба
		offset := uint32(8)
		jumpprobe := filter[offset]
		if jumpprobe == id {
			i = offset
			j = offset
		} else if jumpprobe > id {
			j = offset
		} else {
			// интерполяционная проба границы, здесь x уже точно внутри границ
			min, max := jumpprobe, filter[n-1]
			offset = uint32(float64(n-10) * (float64(id-min) / float64(max-min))) // (n-1 - 8)-1
			probe := filter[offset]
			if probe == id {
				i = offset
				j = offset
			} else if probe < id {
				i = offset
				if ((offset + 32) < n) && (filter[offset+32] > id) {
					j = offset + 32
				}
			} else {
				j = offset
				if (offset >= 32) && (filter[offset-32] < id) {
					i = offset - 32
				}
			}
		}
	}

	for i < j {
		h := (i + j) >> 1
		if filter[h] < id {
			i = h + 1
		} else {
			j = h
		}
	}

	idx := int(i)

	if idx < ln {
		if filter[idx] == id {
			iter.pos = delta + int32(idx)
			iter.lastJumpOk = true
			return true
		}
		if iter.grow > 0 {
			// в этой позиции id уже больше
			iter.pos = delta + int32(idx)
			iter.lastJumpOk = true
			return true
		} else {
			iter.pos = delta + int32(idx)
			iter.lastJumpOk = iter.HasNext()
			return iter.lastJumpOk
		}
	}
	if iter.grow < 0 {
		iter.pos = delta + int32(idx)
		iter.lastJumpOk = iter.HasNext()
		return iter.lastJumpOk
	}
	iter.lastJumpOk = false
	return false
}

func (iter *RangeIterator) HasNext() bool {
	if iter.maxpos < 0 {
		return false
	}
	iter.pos += iter.grow
	return iter.pos >= iter.minpos && iter.pos <= iter.maxpos
}

func (iter *RangeIterator) NextID() IDAcc {
	if iter.pos >= iter.minpos && iter.pos <= iter.maxpos {
		return iter.filter[iter.pos]
	}
	return 0
}

type IntersectIterator struct {
	// сортирован по увеличению длины, последний итератор - самый длинный
	iterators    []IDIterator
	iterdiffs    []IDIterator
	currid       IDAcc
	reversed     bool //у всех iterators он дожен быть такой же
	lastJumpTo   IDAcc
	lastJumpOk   bool
	notIntersect bool
}

func NewIteratorIntersect(reversed bool) *IntersectIterator {
	return &IntersectIterator{
		iterators: make([]IDIterator, 0, 10),
		iterdiffs: make([]IDIterator, 0, 2),
		reversed:  reversed,
	}
}

func (iter *IntersectIterator) Clone() IDIterator {
	rv := &IntersectIterator{}
	*rv = *iter
	return rv
}

func (iter *IntersectIterator) Append(iterator IDIterator) {
	if iterator == nil {
		return
	}
	if iter.reversed != iter.Reversed() {
		panic("iterators have different reverse order")
	}
	ln := len(iter.iterators)
	idx := sort.Search(ln, func(i int) bool {
		// от меньшего множества к большему
		return iter.iterators[i].Cardinality() >= iterator.Cardinality()
	})
	iter.iterators = append(iter.iterators, iterator)
	if idx < ln {
		copy(iter.iterators[idx+1:], iter.iterators[idx:])
		iter.iterators[idx] = iterator
	}
	// тут еще предварительная проверка краев, чтобы не делать полные проходы совсем не пересекающихся множеств

check:
	for i, it := range iter.iterators {
		imin, imax := it.Range()
		if imin == 0 && imax == 0 {
			iter.notIntersect = true
			break check
		}
		for j := i + 1; j < len(iter.iterators); j++ {
			jmin, jmax := iter.iterators[j].Range()
			if jmin == 0 && jmax == 0 {
				iter.notIntersect = true
				break check
			}
			// imin imax < jmin jmax
			// jmin jmax < imin imax
			if jmax < imin || jmin > imax {
				iter.notIntersect = true
				break check
			}
		}
	}
}

// at least one iterator needed for successful difference
func (iter *IntersectIterator) AppendDiff(iterator IDIterator) {
	if iterator == nil {
		return
	}
	if iter.reversed != iter.Reversed() {
		panic("iterators have different reverse order")
	}
	ln := len(iter.iterdiffs)
	idx := sort.Search(ln, func(i int) bool {
		// здесь наоборот, идем от большего множества к меньшему
		return iter.iterdiffs[i].Cardinality() <= iterator.Cardinality()
	})
	iter.iterdiffs = append(iter.iterdiffs, iterator)
	if idx < ln {
		copy(iter.iterdiffs[idx+1:], iter.iterdiffs[idx:])
		iter.iterdiffs[idx] = iterator
	}
}

func (iter *IntersectIterator) Size() int {
	return len(iter.iterators)
}

func (iter *IntersectIterator) SizeDiffs() int {
	return len(iter.iterdiffs)
}

func (iter *IntersectIterator) Iter(n int) IDIterator {
	return iter.iterators[n]
}

func (iter *IntersectIterator) IterDiff(n int) IDIterator {
	return iter.iterdiffs[n]
}

func (iter *IntersectIterator) JumpTo(id IDAcc) bool {
	if iter.lastJumpTo == id {
		return iter.lastJumpOk
	}
	iter.lastJumpTo = id

	neq := false
	eqid := id

	for i, it := range iter.iterators {
		ok := it.JumpTo(id)
		if !ok {
			iter.lastJumpOk = false
			return false
		}
		if i == 0 {
			eqid = it.NextID()
		} else if !neq {
			v := it.NextID()
			if v != eqid {
				neq = true
			}
		}
	}

	for _, it := range iter.iterdiffs {
		ok := it.JumpTo(id)
		if ok && !neq {
			v := it.NextID()
			if v == eqid {
				neq = true
			}
		}
	}

	if neq {
		ok := iter.HasNext()
		iter.lastJumpOk = ok
		return ok
	}

	iter.currid = eqid
	iter.lastJumpOk = true
	return true
}

func (iter *IntersectIterator) Cardinality() int32 {
	// не может быть длинее самого короткого (индекс 0) из итераторов
	return iter.iterators[0].Cardinality()
}

func (iter *IntersectIterator) Range() (IDAcc, IDAcc) {
	return iter.iterators[0].Range()
}

func (iter *IntersectIterator) Reversed() bool {
	return iter.reversed
}

func (iter *IntersectIterator) HasNext() bool {

	if iter.notIntersect {
		return false
	}

	// из всех NextID берем max (если у всех reverse=false) или min (у всех reverse=true) и подводим к нему все остальные
	// если все равны - это нужное нам значение

retry:
	for _, it := range iter.iterators {
		if !it.HasNext() {
			return false
		}
	}

	// значение из первого итератора
	it0 := iter.iterators[0]
	cmpID := it0.NextID()
	// стартуем со второго итератора
	iidx := 1
	for {
		if iidx >= len(iter.iterators) {
			for _, it := range iter.iterdiffs {
				ok := it.JumpTo(cmpID)
				if ok {
					if it.NextID() == cmpID {
						goto retry
					}
				}
			}
			break
		}
		it := iter.iterators[iidx]
		v := it.NextID()
		if v == cmpID {
			iidx++
		} else {
			if !iter.reversed {
				if v > cmpID {
					if !it0.JumpTo(v) {
						return false
					}
					cmpID = it0.NextID()
					iidx = 1
				} else {
					// v < cmpID
					if !it.JumpTo(cmpID) {
						return false
					}
				}
			} else {
				// reversed
				if v < cmpID {
					if !it0.JumpTo(v) {
						return false
					}
					cmpID = it0.NextID()
					iidx = 1
				} else {
					// v > cmpID
					if !it.JumpTo(cmpID) {
						return false
					}
				}
			}
		}
	}
	iter.currid = cmpID
	return iidx >= len(iter.iterators)
}

func (iter *IntersectIterator) NextID() IDAcc {
	return iter.currid
}

type MergeIterator struct {
	// сортирован по увеличению длины, последний итератор - самый длинный
	iterators   []IDIterator
	currid      IDAcc
	minheap     *IDAccHeap
	cardinality int32
	reversed    bool //у всех iterators он дожен быть такой же
	min, max    IDAcc
	lastJumpTo  IDAcc
	lastJumpOk  bool
}

func NewMergeIterator(iterators ...IDIterator) *MergeIterator {
	if len(iterators) == 0 {
		panic("iterators not defined")
	}

	reversed := iterators[0].Reversed()
	h := NewIDAccHeap(reversed, len(iterators))
	InitIDAccHeap(h)
	maxSz := int32(0)
	var l, r IDAcc

	for i, it := range iterators {
		if it == nil {
			continue
		}
		if reversed != it.Reversed() {
			panic("iterators have different reverse order")
		}
		il, ir := it.Range()
		if i == 0 || l > il {
			l = il
		}
		if i == 0 || r < ir {
			r = ir
		}
		lenList := it.Cardinality()
		if lenList > maxSz {
			maxSz = lenList
		}
		if it.HasNext() {
			PushIDAccHeap(h, ElemHeapIDAcc{
				ID:       it.NextID(),
				Iterator: it,
			})
		}
	}

	return &MergeIterator{
		iterators:   iterators,
		reversed:    reversed,
		minheap:     h,
		min:         l,
		max:         r,
		cardinality: maxSz,
	}
}

func (iter *MergeIterator) Clone() IDIterator {
	rv := &MergeIterator{}
	*rv = *iter
	rv.minheap = iter.minheap.Clone()
	rv.iterators = make([]IDIterator, len(iter.iterators))
	// итераторы были клонированы при клонировании minheap
	for i := range rv.minheap.Elems {
		rv.iterators[i] = rv.minheap.Elems[i].Iterator
	}
	return rv
}

func (iter *MergeIterator) JumpTo(id IDAcc) bool {
	if iter.lastJumpTo == id {
		return iter.lastJumpOk
	}
	iter.lastJumpTo = id

	iter.minheap.Elems = iter.minheap.Elems[:0]

	ok := false

	for _, it := range iter.iterators {
		if it == nil {
			continue
		}
		cok := it.JumpTo(id)
		ok = ok || cok
		if cok {
			PushIDAccHeap(iter.minheap, ElemHeapIDAcc{
				ID:       it.NextID(),
				Iterator: it,
			})
		}
	}
	if ok {
		iter.currid = iter.minheap.Elems[0].ID
	} else {
		iter.currid = 0
	}
	iter.lastJumpOk = ok
	return ok
}

func (iter *MergeIterator) Cardinality() int32 {
	return iter.cardinality
}

func (iter *MergeIterator) Range() (l IDAcc, r IDAcc) {
	return iter.min, iter.max
}

func (iter *MergeIterator) Reversed() bool {
	return iter.reversed
}

func (iter *MergeIterator) HasNext() bool {
	for iter.minheap.Len() > 0 {
		me := iter.minheap.Elems[0] // Peek at the top element in heap.
		if iter.currid == 0 || me.ID != iter.currid {
			iter.currid = me.ID // Add if unique.
			return true
		}
		if !me.Iterator.HasNext() {
			PopIDAccHeap(iter.minheap)
		} else {
			val := me.Iterator.NextID()
			iter.minheap.Elems[0].ID = val
			FixIDAccHeap(iter.minheap, 0) // Faster than Pop() followed by Push().
		}
	}
	return false
}

func (iter *MergeIterator) NextID() IDAcc {
	return iter.currid
}

// REQUEST  URI: /accounts/filter/?query_id=826&sex_eq=m&interests_any=%D0%A2%D1%83%D1%84%D0%BB%D0%B8&status_neq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&limit=4
// REQUEST BODY: <EMPTY>
// BODY   GOT: {"accounts":[{"id":29931,"email":"sedtunes@inbox.ru","sex":"m","status":"заняты"},{"id":29873,"email":"neledpul@inbox.ru","sex":"m","status":"заняты"},{"id":29809,"email":"hudticuteenniot@yahoo.com","sex":"m","status":"всё сложно"},{"id":29807,"email":"herilonmahepus@list.ru","sex":"m","status":"свободны"}]}
// BODY   EXP: {"accounts":[{"sex":"m","email":"sedtunes@inbox.ru","status":"заняты","id":29931},{"sex":"m","email":"neledpul@inbox.ru","status":"заняты","id":29873},{"sex":"m","email":"hudticuteenniot@yahoo.com","status":"всё сложно","id":29809},{"sex":"m","email":"omalesrenelbinot@yandex.ru","status":"заняты","id":29627}]}
