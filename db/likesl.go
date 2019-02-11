package db

import (
	"sync"
	"sync/atomic"
)

var lkPool = sync.Pool{}

func GetLikesSlice(c int) []Like {
	sl := lkPool.Get()
	if sl != nil {
		vsl := sl.([]Like)
		if cap(vsl) >= int(c) {
			return vsl
		}
	}
	return make([]Like, 0, c)
}

func PutLikesSlice(sl []Like) {
	if sl == nil {
		return
	}
	lkPool.Put(sl[:0])
}

type Like struct {
	likerhi byte
	likerlo uint16
	likedhi byte
	likedlo uint16
	Stamp   int32
}

func (lk Like) Liker() IDAcc {
	return IDAcc(lk.likerhi)<<16 | IDAcc(lk.likerlo)
}

func (lk Like) Liked() IDAcc {
	return IDAcc(lk.likedhi)<<16 | IDAcc(lk.likedlo)
}

func NewLike(liker, liked IDAcc, stamp int32) Like {
	lk := Like{
		likerhi: byte(liker >> 16),
		likerlo: uint16(liker),
		likedhi: byte(liked >> 16),
		likedlo: uint16(liked),
		Stamp:   stamp,
	}
	return lk
}

type DictLikeIndex int32

type DictLikes struct {
	curridx int32
	likes   []Like
}

func NewDictLikes(c int) *DictLikes {
	return &DictLikes{
		likes:   make([]Like, 0, c),
		curridx: -1,
	}
}

func (dl *DictLikes) Add(like Like) DictLikeIndex {
	didx := DictLikeIndex(atomic.AddInt32(&(dl.curridx), 1))
	dl.likes = append(dl.likes, like)
	return didx
}

func (dl *DictLikes) Get(idx DictLikeIndex) Like {
	ret := dl.likes[idx]
	return ret
}

func (dl *DictLikes) GetLiker(idx DictLikeIndex) IDAcc {
	ret := dl.likes[idx].Liker()
	return ret
}

func (dl *DictLikes) GetLiked(idx DictLikeIndex) IDAcc {
	ret := dl.likes[idx].Liked()
	return ret
}

func (dl *DictLikes) GetLikeStamp(idx DictLikeIndex) int32 {
	ret := dl.likes[idx].Stamp
	return ret
}

type AddLike struct {
	like  Like
	idx   DictLikeIndex
	chout chan DictLikeIndex
}

const oneLikeCap = 760

type LikeSlice struct {
	dictLikes  *DictLikes
	chin_liker chan AddLike
	chin_liked chan AddLike
	likes      [65536][]DictLikeIndex
}

func NewLikeSlice(dictLikes *DictLikes) *LikeSlice {
	ls := &LikeSlice{
		dictLikes:  dictLikes,
		chin_liker: make(chan AddLike, 10),
		chin_liked: make(chan AddLike, 10),
	}
	for i := 0; i < 65536; i++ {
		ls.likes[i] = make([]DictLikeIndex, 0, oneLikeCap)
	}
	go ls.workerAdd()
	return ls
}

func (ls *LikeSlice) workerAdd() {
	for {
		select {
		case chlike := <-ls.chin_liker:
			like := chlike.like
			chunk := like.Liker() & 0xffff
			a := ls.likes[chunk]
			n := uint32(len(a))
			i, j := uint32(0), n
			for i < j {
				h := (i + j) >> 1
				cell := ls.dictLikes.likes[a[h]]
				lkr1 := cell.Liker()
				lkd1 := cell.Liked()
				lkr2 := like.Liker()
				lkd2 := like.Liked()
				if lkr1 < lkr2 || (lkr1 == lkr2 && lkd1 <= lkd2) {
					i = h + 1
				} else {
					j = h
				}
			}
			var didx DictLikeIndex
			if chlike.idx < 0 {
				didx = ls.dictLikes.Add(like)
			} else {
				didx = chlike.idx
			}
			a = append(a, didx)
			if i < n {
				copy(a[i+1:], a[i:])
				a[i] = didx
			}
			ls.likes[chunk] = a
			chlike.chout <- didx
		case chlike := <-ls.chin_liked:
			like := chlike.like
			chunk := like.Liked() & 0xffff
			a := ls.likes[chunk]
			n := uint32(len(a))
			i, j := uint32(0), n
			for i < j {
				h := (i + j) >> 1
				cell := ls.dictLikes.likes[a[h]]
				lkr1 := cell.Liker()
				lkd1 := cell.Liked()
				lkr2 := like.Liker()
				lkd2 := like.Liked()
				if lkd1 < lkd2 || (lkd1 == lkd2 && lkr1 <= lkr2) {
					i = h + 1
				} else {
					j = h
				}
			}
			var didx DictLikeIndex
			if chlike.idx < 0 {
				didx = ls.dictLikes.Add(like)
			} else {
				didx = chlike.idx
			}
			a = append(a, didx)
			if i < n {
				copy(a[i+1:], a[i:])
				a[i] = didx
			}
			ls.likes[chunk] = a
			chlike.chout <- didx
		}
	}
}

func (ls *LikeSlice) AddLikes(like Like, idx DictLikeIndex) chan DictLikeIndex {
	chout := make(chan DictLikeIndex, 1)
	ls.chin_liker <- AddLike{like, idx, chout}
	return chout
}

func (ls *LikeSlice) AddLiked(like Like, idx DictLikeIndex) chan DictLikeIndex {
	chout := make(chan DictLikeIndex, 1)
	ls.chin_liked <- AddLike{like, idx, chout}
	return chout
}

func (ls *LikeSlice) Likes(id IDAcc) []DictLikeIndex {
	chunk := id & 0xffff
	a := ls.likes[chunk]
	ln := len(a)

	n := uint32(len(a))
	i, j := uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if ls.dictLikes.likes[a[h]].Liker() < id {
			i = h + 1
		} else {
			j = h
		}
	}
	idx := int(i)

	if idx >= ln {
		return nil
	}
	if ls.dictLikes.likes[a[idx]].Liker() != id {
		return nil
	}
	ifrom := idx

	b := a[ifrom:]
	n = uint32(len(b))
	i, j = uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if ls.dictLikes.likes[b[h]].Liker() <= id {
			i = h + 1
		} else {
			j = h
		}
	}
	idx = int(i)
	return b[:idx]
}

func (ls *LikeSlice) Liked(id IDAcc) []DictLikeIndex {
	chunk := id & 0xffff
	a := ls.likes[chunk]
	ln := len(a)

	n := uint32(len(a))
	i, j := uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if ls.dictLikes.likes[a[h]].Liked() < id {
			i = h + 1
		} else {
			j = h
		}
	}
	idx := int(i)

	if idx >= ln {
		return nil
	}
	if ls.dictLikes.likes[a[idx]].Liked() != id {
		return nil
	}
	ifrom := idx

	b := a[ifrom:]
	n = uint32(len(b))
	i, j = uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if ls.dictLikes.likes[b[h]].Liked() <= id {
			i = h + 1
		} else {
			j = h
		}
	}
	idx = int(i)
	return b[:idx]
}

type SortByLiker []Like

func (p SortByLiker) Len() int           { return len(p) }
func (p SortByLiker) Less(i, j int) bool { return p[i].Liker() < p[j].Liker() }
func (p SortByLiker) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type SortByLiked []Like

func (p SortByLiked) Len() int           { return len(p) }
func (p SortByLiked) Less(i, j int) bool { return p[i].Liked() < p[j].Liked() }
func (p SortByLiked) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type SortByLikedReverse []Like

func (p SortByLikedReverse) Len() int           { return len(p) }
func (p SortByLikedReverse) Less(i, j int) bool { return p[i].Liked() > p[j].Liked() }
func (p SortByLikedReverse) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
