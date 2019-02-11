package db

import (
	"github.com/covrom/highloadcup2018/dict"
	"sync"
	// _ "github.com/lib/pq"
)

var CurrentTime int32

var SmAcc = NewLocalStore()

func NewLocalStore() *SmallAccounts {
	return NewSmallAccounts(1600000)
}

type SmallAccount struct {
	ID            IDAcc
	Phone         string
	Sex           byte
	Birth         int32
	Joined        int32
	PremiumStart  int32
	PremiumFinish int32
}

type SmallAccounts struct {
	sync.RWMutex
	accs []SmallAccount
	byid []int32 // индексы в accs по ID

	Domain                   *Column
	Sex                      *Column // 0 = female, 1 = male
	FirstName                *Column
	SecondName               *Column
	Country                  *Column
	City                     *Column
	Status                   *Column
	BirthYear                *Column
	JoinedYear               *Column
	Interests                *Column
	SexPremiumStatusInterest *Column
	PhoneCode                *Column

	// повышаем селективность
	StatusCity    *Column // city<<2|status
	StatusCountry *Column // country<<2|status
	SexCity       *Column // city<<1|sex
	SexCountry    *Column // country<<1|sex

	dictLikes *DictLikes
	likes     *LikeSlice // сорт. по убыванию Liker
	liked     *LikeSlice // сорт. по убыванию Liked
}

func NewSmallAccounts(c int) *SmallAccounts {
	dictLikes := NewDictLikes(50000000)
	dictInterests := dict.NewDictonary(100)
	emptyInterest := DataEntry(dictInterests.Put(""))

	return &SmallAccounts{
		accs: make([]SmallAccount, 0, c),
		byid: make([]int32, 0, c),

		dictLikes: dictLikes,
		likes:     NewLikeSlice(dictLikes),
		liked:     NewLikeSlice(dictLikes),

		Domain:     NewColumnZeroString(1600000, 20, false, ""),
		Sex:        NewColumnZeroDataEntry(1600000, 2, false, nil, 0),
		FirstName:  NewColumnZeroString(1600000, 200, false, ""),
		SecondName: NewColumnZeroString(1600000, 2000, false, ""),
		Country:    NewColumnZeroString(1600000, 100, false, ""),
		City:       NewColumnZeroString(1600000, 1000, false, ""),
		PhoneCode:  NewColumnZeroString(1600000, 150, false, ""),
		Status:     NewColumnZeroString(1600000, 3, false, ""),
		BirthYear:  NewColumnZeroDataEntry(1600000, 50, false, nil, DataEntry(NullTime)),
		JoinedYear: NewColumnZeroDataEntry(1600000, 50, false, nil, DataEntry(NullTime)),
		Interests:  NewColumnZeroDataEntry(1600000, 100, true, dictInterests, emptyInterest),

		StatusCity:               NewColumnZeroDataEntry(1600000, 4000, false, nil, 0),
		StatusCountry:            NewColumnZeroDataEntry(1600000, 400, false, nil, 0),
		SexCity:                  NewColumnZeroDataEntry(1600000, 2000, false, nil, 0),
		SexCountry:               NewColumnZeroDataEntry(1600000, 200, false, nil, 0),
		SexPremiumStatusInterest: NewColumnZeroDataEntry(1600000, 1600, true, nil, 0),
	}
}

func (ls *SmallAccounts) Append(acc SmallAccount, likes []Like) {
	ls.Set(acc, likes)
}

func (sas *SmallAccounts) Set(acc SmallAccount, likes []Like) int32 {
	iid := int(acc.ID)
	if iid < len(sas.byid) {
		idx := sas.byid[iid]
		if idx < 0 {
			idx = int32(len(sas.accs))
			sas.accs = append(sas.accs, acc)
			sas.byid[iid] = idx
			for _, like := range likes {
				sas.AddLike(like)
			}
			return idx
		} else {
			sas.accs[idx] = acc
			for _, like := range likes {
				sas.AddLike(like)
			}
			return idx
		}
	} else {
		for len(sas.byid) <= iid {
			sas.byid = append(sas.byid, -1)
		}
		idx := int32(len(sas.accs))
		sas.accs = append(sas.accs, acc)
		sas.byid[iid] = idx
		for _, like := range likes {
			sas.AddLike(like)
		}
		return idx
	}
}

func (sas *SmallAccounts) GetLikeLiker(v DictLikeIndex) IDAcc {
	return sas.dictLikes.GetLiker(v)
}

func (sas *SmallAccounts) GetLikeLiked(v DictLikeIndex) IDAcc {
	return sas.dictLikes.GetLiked(v)
}

func (sas *SmallAccounts) GetLikeStamp(v DictLikeIndex) int32 {
	return sas.dictLikes.GetLikeStamp(v)
}

func (sas *SmallAccounts) GetLike(v DictLikeIndex) Like {
	return sas.dictLikes.Get(v)
}

func (sas *SmallAccounts) LikesDict(id IDAcc) []DictLikeIndex {
	return sas.likes.Likes(id)
}

func (sas *SmallAccounts) Likes(id IDAcc) (res []Like) {
	didxs := sas.likes.Likes(id)
	res = GetLikesSlice(len(didxs))
	for _, v := range didxs {
		res = append(res, sas.dictLikes.Get(v))
	}
	return
}

func (sas *SmallAccounts) LikedDict(id IDAcc) []DictLikeIndex {
	return sas.liked.Liked(id)
}

func (sas *SmallAccounts) Liked(id IDAcc) (res []Like) {
	didxs := sas.liked.Liked(id)
	res = GetLikesSlice(len(didxs))
	for _, v := range didxs {
		res = append(res, sas.dictLikes.Get(v))
	}
	return
}

func (sas *SmallAccounts) AddLike(like Like) {
	didx := <-sas.likes.AddLikes(like, -1)
	<-sas.liked.AddLiked(like, didx)
}

func (sas *SmallAccounts) Contains(id IDAcc) bool {
	iid := int(id)
	if iid < len(sas.byid) {
		return sas.byid[iid] >= 0
	} else {
		return false
	}
}

func (sas *SmallAccounts) Length() int {
	return len(sas.accs)
}

func (sas *SmallAccounts) Get(idx int32) SmallAccount {
	return sas.accs[idx]
}

func (sas *SmallAccounts) GetIdx(id IDAcc) int32 {
	iid := int(id)
	if iid < len(sas.byid) {
		return sas.byid[iid]
	} else {
		return -1
	}
}

func (sas *SmallAccounts) GetById(id IDAcc) (SmallAccount, bool) {
	iid := int(id)
	if iid < len(sas.byid) {
		idx := sas.byid[iid]
		if idx < 0 {
			return SmallAccount{}, false
		}
		return sas.accs[idx], true
	} else {
		return SmallAccount{}, false
	}
}

// удаляется только в карте id-idx, а не в самом исходном массиве
func (sas *SmallAccounts) Delete(id IDAcc) {
	iid := int(id)
	if iid < len(sas.byid) {
		sas.byid[iid] = -1
	}
}

func (sas *SmallAccounts) Iterator() *SmallAccountsIterator {
	return &SmallAccountsIterator{
		pos: len(sas.byid),
		sas: sas,
	}
}

// filter должен быть отсортирован по возрастанию
func (sas *SmallAccounts) IteratorWithFilter(filter []IDAcc) *SmallAccountsIterator {
	return &SmallAccountsIterator{
		pos:    len(filter),
		sas:    sas,
		filter: filter,
	}
}

// в порядке убывания, после Reset

type SmallAccountsIterator struct {
	pos    int
	sas    *SmallAccounts
	filter []IDAcc
}

func (iter *SmallAccountsIterator) Clone() IDIterator {
	rv := &SmallAccountsIterator{}
	*rv = *iter
	return rv
}

func (iter *SmallAccountsIterator) Cardinality() int32 {
	if len(iter.filter) > 0 {
		return int32(len(iter.filter))
	} else {
		return int32(len(iter.sas.byid))
	}
}

func (iter *SmallAccountsIterator) Range() (IDAcc, IDAcc) {
	var a, b IDAcc
	if len(iter.filter) > 0 {
		a, b = iter.filter[0], iter.filter[len(iter.filter)-1]
	} else {
		a, b = IDAcc(0), IDAcc(len(iter.sas.byid)-1)
	}
	if a > b {
		a, b = b, a
	}
	return a, b
}

func (iter *SmallAccountsIterator) Reversed() bool {
	return true
}

func (iter *SmallAccountsIterator) JumpTo(id IDAcc) bool {
	if len(iter.filter) > 0 {
		filter := iter.filter
		delta := 0
		if iter.pos >= 0 && iter.sas.byid[filter[iter.pos]] < 0 {
			if filter[iter.pos] < id {
				filter = filter[iter.pos:]
				delta = iter.pos
			} else if filter[iter.pos] > id {
				filter = filter[:iter.pos+1]
			} else {
				return true
			}
		}

		ln := len(filter)
		// проверим границы
		if ln == 0 {
			return false
		}
		if filter[0] > id {
			return false
		}

		n := uint32(ln)
		i, j := uint32(0), n
		for i < j {
			h := (i + j) >> 1
			if filter[h] < id {
				i = h + 1
			} else {
				j = h
			}
		}
		if i < n {
			if filter[i] == id {
				iter.pos = delta + int(i)
				return true
			}
			if i == 0 {
				return false
			}
			iter.pos = delta + int(i)
			return iter.HasNext()
		}
		return false
	} else {
		if int(id) >= len(iter.sas.byid) {
			return false
		}
		idx := int(id)
		for idx >= 0 && iter.sas.byid[idx] < 0 {
			idx--
		}
		if idx < 0 {
			return false
		}
		iter.pos = idx
		return true
	}
}

func (iter *SmallAccountsIterator) HasNext() bool {
	iter.pos--
	if len(iter.filter) > 0 {
		for iter.pos >= 0 && iter.sas.byid[iter.filter[iter.pos]] < 0 {
			iter.pos--
		}
	} else {
		for iter.pos >= 0 && iter.sas.byid[iter.pos] < 0 {
			iter.pos--
		}
	}
	return iter.pos >= 0
}

func (iter *SmallAccountsIterator) Next() SmallAccount {
	if len(iter.filter) > 0 {
		return iter.sas.accs[iter.sas.byid[iter.filter[iter.pos]]]
	} else {
		return iter.sas.accs[iter.sas.byid[iter.pos]]
	}
}

func (iter *SmallAccountsIterator) NextID() IDAcc {
	if len(iter.filter) > 0 {
		return iter.filter[iter.pos]
	} else {
		return IDAcc(iter.pos)
	}
}
