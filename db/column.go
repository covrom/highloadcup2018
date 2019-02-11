package db

import (
	"github.com/covrom/highloadcup2018/dict"
	"sync"
)

type DataEntry int32

const NullEntry DataEntry = -1

const bucketsCount = 1 << 16

func remFunc(v uint32) (uint16, byte) { return uint16(v & 0xffff), byte(v >> 16) }

func valFunc(b uint16, h byte) uint32 { return (uint32(h) << 16) | uint32(b) }

type valEntry struct {
	rem byte
	ids []IDAcc
}

type kvSet struct {
	id  IDAcc
	val DataEntry
}

type Column struct {
	sync.RWMutex
	// кластерный индекс, сортирован в порядке возрастания ключа (ID)
	// индекс коллекции - это ID
	// могут быть пропуски ID, в них DataEntry==empty
	cluster    []DataEntry
	clusterset [][]DataEntry
	// индекс, по значению (DataEntry), отсортирован только в рамках одного bucket
	// все одинаковые значения находятся в одном bucket
	// позволяет быстро найти по значению все ID, отсортированные по возрастанию
	// индекс коллекции - значение DataEntry
	values [][]valEntry

	dict *dict.LeafDictonary

	useval bool
	use1b  bool     // биткарта, 1 бит на значение
	use2b  bool     // биткарта, 2 бит на значение
	use4b  bool     // биткарта, 4 бит на значение
	useset bool     // в качестве значения val по ID - массив с сохранением порядка, а не одиночное значение
	bmp    []uint64 // биткарта
	count  []int32  // количества по idx=val

	minId IDAcc
	maxId IDAcc

	empty DataEntry // для use1b Contains работает просто как проверка границ, для остальных - проверяет на это пустое значение

	chset chan kvSet
}

func NewColumnZeroString(lines, vals int, useset bool, zeroval string) *Column {
	dct := dict.NewDictonary(vals)
	return NewColumnZeroDataEntry(lines, vals, useset, dct, DataEntry(dct.Put(zeroval)))
}

func NewColumnZeroDataEntry(lines, vals int, useset bool, dct *dict.LeafDictonary, zeroval DataEntry) *Column {
	ret := &Column{
		minId: 0xffffffff,
		dict:  dct,
		empty: zeroval,
		chset: make(chan kvSet, 1000),
	}
	if useset {
		ret.useset = true
		ret.clusterset = make([][]DataEntry, 0, lines)
		ret.values = make([][]valEntry, bucketsCount)
	} else if vals <= 2 {
		ret.use1b = true
		ret.bmp = make([]uint64, 1+(lines>>6))
		ret.count = make([]int32, 2)
	} else if vals <= 4 {
		ret.use2b = true
		ret.bmp = make([]uint64, 1+(lines>>5))
		ret.count = make([]int32, 4)
	} else if vals <= 16 {
		ret.use4b = true
		ret.bmp = make([]uint64, 1+(lines>>4))
		ret.count = make([]int32, 16)
	} else {
		ret.cluster = make([]DataEntry, 0, lines)
		ret.values = make([][]valEntry, bucketsCount)
		ret.useval = true
	}
	go ret.workerSet()
	return ret
}

func (c *Column) workerSet() {
	for kv := range c.chset {
		c.set(kv.id, kv.val)
	}
}

func (c *Column) SetEntrySet(id IDAcc, set []DataEntry, upd, async bool) {
	if c.useset {
		c.setCluster(id, 0, true)
		for _, v := range set {
			c.Set(id, v, upd, async)
		}
		return
	}
	panic("not useset")
}

func (c *Column) SetString(id IDAcc, v string, upd, async bool) {
	if len(v) == 0 {
		c.Set(id, c.empty, upd, async)
	} else {
		c.Set(id, DataEntry(c.dict.Put(v)), upd, async)
	}
}

func (c *Column) SetStringSet(id IDAcc, v []string, upd, async bool) {
	if c.useset {
		c.setCluster(id, 0, true)
		for _, vv := range v {
			c.Set(id, DataEntry(c.dict.Put(vv)), upd, async)
		}
		return
	}
	panic("not useset")
}

func binSearchValEntry(a []valEntry, x byte) uint32 {
	n := uint32(len(a))
	i, j := uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if a[h].rem < x {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func binSearchIDAcc(a []IDAcc, x IDAcc) uint32 {
	n := uint32(len(a))
	i, j := uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if a[h] < x {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func binApproxSearchIDAcc(a []IDAcc, x IDAcc) uint32 {
	n := uint32(len(a))
	if n == 0 {
		return 0
	}
	min, max := a[0], a[n-1]
	if x < min {
		return 0
	} else if x > max {
		return n
	}
	i, j := uint32(0), n
	if n > 96 {
		// интерполяционная проба границы, здесь x уже точно внутри границ
		offset := uint32(float64(n-1) * (float64(x-min) / float64(max-min)))
		probe := a[offset]
		if probe == x {
			return offset
		} else if probe < x {
			i = offset
			if (offset < 32) && (a[offset+32] > x) {
				j = offset + 32
			}
		} else {
			j = offset
			if (n-offset < 32) && (a[offset-32] < x) {
				i = offset - 32
			}
		}
	}
	for i < j {
		h := (i + j) >> 1
		if a[h] < x {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func (c *Column) setCluster(id IDAcc, v DataEntry, clearset bool) {
	if c.useset {
		for uint32(len(c.clusterset)) <= uint32(id) {
			c.clusterset = append(c.clusterset, make([]DataEntry, 0, 10))
		}
		if clearset {
			a := c.clusterset[uint32(id)]
			for _, oldv := range a {
				bck, rem := remFunc(uint32(oldv))
				cv := c.values[bck]
				ln := len(cv)
				ii := int(binSearchValEntry(cv, rem))
				if ii < ln && cv[ii].rem == rem {
					lnids := len(cv[ii].ids)
					iids := int(binApproxSearchIDAcc(cv[ii].ids, id))
					if iids < lnids && cv[ii].ids[iids] == id {
						if iids < lnids-1 {
							copy(cv[ii].ids[iids:], cv[ii].ids[iids+1:])
						}
						cv[ii].ids = cv[ii].ids[:lnids-1]
					}
					c.values[bck] = cv
				}
			}
			if a == nil {
				c.clusterset[uint32(id)] = make([]DataEntry, 0, 10)
			} else {
				c.clusterset[uint32(id)] = a[:0]
			}
		} else {
			c.clusterset[uint32(id)] = append(c.clusterset[uint32(id)], v)
		}
	} else {
		for uint32(len(c.cluster)) <= uint32(id) {
			c.cluster = append(c.cluster, NullEntry)
		}
		c.cluster[uint32(id)] = v
	}
}

// нельзя вызывать для бинарных и сетов!
func (c *Column) Delete(id IDAcc, oldv DataEntry) {
	bck, rem := remFunc(uint32(oldv))
	cv := c.values[bck]
	ln := len(cv)
	ii := int(binSearchValEntry(cv, rem))
	if ii < ln && cv[ii].rem == rem {
		lnids := len(cv[ii].ids)
		iids := int(binApproxSearchIDAcc(cv[ii].ids, id))
		if iids < lnids && cv[ii].ids[iids] == id {
			if iids < lnids-1 {
				copy(cv[ii].ids[iids:], cv[ii].ids[iids+1:])
			}
			cv[ii].ids = cv[ii].ids[:lnids-1]
		}
		c.values[bck] = cv
		c.setCluster(id, NullEntry, false)
	}
}

func (c *Column) set(id IDAcc, v DataEntry) {
	if c.maxId < id {
		c.maxId = id
	}

	if c.minId > id {
		c.minId = id
	}

	if c.use1b {
		pos, sub := id>>6, id&0x3f
		mask := uint64(1) << sub
		if v == 1 {
			c.bmp[pos] |= mask
		} else {
			c.bmp[pos] &^= mask
		}
		c.count[v]++
		return
	} else if c.use2b {
		pos, sub := id>>5, id&0x1f
		mask := uint64(3) << (sub * 2)
		c.bmp[pos] &^= mask
		c.bmp[pos] |= (uint64(v) & 0x3) << (sub * 2)
		c.count[v]++
		return
	} else if c.use4b {
		pos, sub := id>>4, id&0x0f
		mask := uint64(0x0f) << (sub * 4)
		c.bmp[pos] &^= mask
		c.bmp[pos] |= (uint64(v) & 0x0f) << (sub * 4)
		c.count[v]++
		return
	}

	bck, rem := remFunc(uint32(v))
	cv := c.values[bck]
	ln := len(cv)
	ii := int(binSearchValEntry(cv, rem))
	if ii < ln && cv[ii].rem == rem {
		// уже есть значение - пробуем добавить ID
		lnids := len(cv[ii].ids)
		iids := int(binApproxSearchIDAcc(cv[ii].ids, id))
		// если уже есть - не добавляем
		if !(iids < lnids && cv[ii].ids[iids] == id) {
			cv[ii].ids = append(cv[ii].ids, id)
			if iids < lnids {
				copy(cv[ii].ids[iids+1:], cv[ii].ids[iids:])
				cv[ii].ids[iids] = id
			}
		}
	} else {
		cv = append(cv, valEntry{
			rem: rem,
			ids: []IDAcc{id},
		})
		if ii < ln {
			copy(cv[ii+1:], cv[ii:])
			cv[ii] = valEntry{
				rem: rem,
				ids: []IDAcc{id},
			}
		}
	}
	c.values[bck] = cv

	// для useset всегда добавляем
	c.setCluster(id, v, false)
}

func (c *Column) Set(id IDAcc, v DataEntry, upd, async bool) {
	if upd {
		if c.use1b || c.use2b || c.use4b {
			oldv := c.Get(id)
			c.count[oldv]--
		} else if !c.useset {
			oldv := c.Get(id)
			if oldv != NullEntry {
				if v == oldv {
					return
				}
				// удаляем oldv
				c.Delete(id, oldv)
			}
		}
	}

	if async {
		c.chset <- kvSet{id, v}
	} else {
		c.set(id, v)
	}
}

func (c *Column) Get(id IDAcc) DataEntry {
	switch {
	case c.useval:
		return c.cluster[uint32(id)]
	case c.use1b:
		pos, sub := id>>6, id&0x3f
		mask := uint64(1) << sub
		return DataEntry((c.bmp[pos] & mask) >> sub)
	case c.use2b:
		pos, sub := id>>5, id&0x1f
		mask := uint64(3) << (sub * 2)
		return DataEntry((c.bmp[pos] & mask) >> (sub * 2))
	case c.use4b:
		pos, sub := id>>4, id&0x0f
		mask := uint64(0x0f) << (sub * 4)
		return DataEntry((c.bmp[pos] & mask) >> (sub * 4))
	case c.useset:
		panic("useset cannot get single")
	default:
		panic("unknown column for Get")
	}
}

func (c *Column) GetSet(id IDAcc) []DataEntry {
	if c.useset {
		return c.clusterset[uint32(id)]
	}
	panic("not useset in GetSet")
}

func (c *Column) IsZero(v DataEntry) bool {
	return v == c.empty
}

func (c *Column) ZeroVal() DataEntry {
	return c.empty
}

func (c *Column) ToDictonary(s string) DataEntry {
	return DataEntry(c.dict.Put(s))
}

func (c *Column) InDictonary(s string) (DataEntry, bool) {
	i, ok := c.dict.In(s)
	return DataEntry(i), ok
}

func (c *Column) FromDictonary(idx DataEntry) string {
	return c.dict.Get(uint32(idx))
}

func (c *Column) DictonaryCompare(x, y DataEntry) int {
	return c.dict.Compare(uint32(x), uint32(y))
}

func (c *Column) GetString(id IDAcc) string {
	de := c.Get(id)
	if de != NullEntry {
		return c.dict.Get(uint32(de))
	}
	return ""
}

func (c *Column) GetStringSet(id IDAcc) (ret []string) {
	de := c.GetSet(id)
	for _, v := range de {
		ret = append(ret, c.dict.Get(uint32(v)))
	}
	return
}

func (c *Column) DictCardinality() int {
	return c.dict.Length()
}

func (c *Column) Contains(id IDAcc) bool {
	switch {
	case c.use1b:
		return int(id>>6) < len(c.bmp)
	case c.use2b:
		pos, sub := id>>5, id&0x1f
		mask := uint64(3) << (sub * 2)
		return DataEntry((c.bmp[pos]&mask)>>(sub*2)) != 0
	case c.use4b:
		pos, sub := id>>4, id&0x0f
		mask := uint64(0x0f) << (sub * 4)
		return DataEntry((c.bmp[pos]&mask)>>(sub*4)) != 0
	case c.useset:
		v := c.GetSet(id)
		return len(v) != 0
	default:
		v := c.cluster[uint32(id)]
		return !(v == NullEntry || v == c.empty)
	}
}

func (c *Column) GetV(v DataEntry) []IDAcc {
	if c.use1b || c.use2b || c.use4b {
		panic("GetV is not defined for bitmap columns")
	}
	bck, rem := remFunc(uint32(v))
	cv := c.values[bck]
	ln := len(cv)
	ii := int(binSearchValEntry(cv, rem))
	if ii < ln && cv[ii].rem == rem {
		return cv[ii].ids
	}
	return nil
}

func (c *Column) GetCountV(v DataEntry) int32 {
	if c.use1b || c.use2b || c.use4b {
		return c.count[v]
	}
	return int32(len(c.GetV(v)))
}

func (c *Column) RangeVals(f func(v DataEntry, ids []IDAcc)) {
	if c.use1b || c.use2b || c.use4b {
		panic("RangeVals is not defined for bitmap columns")
	} else {
		for i, bucket := range c.values {
			for _, val := range bucket {
				f(DataEntry(valFunc(uint16(i), val.rem)), val.ids)
			}
		}
	}
}
