package queryset

import (
	"strconv"
	"strings"
	"sync"

	"github.com/covrom/highloadcup2018/account"
	"github.com/covrom/highloadcup2018/db"
	"github.com/covrom/highloadcup2018/dict"

	"github.com/RoaringBitmap/roaring"
)

type ValidVals struct {
	email_lt,
	email_gt,
	sname_starts string

	sex_eq,
	fname_null,
	sname_null,
	phone_null,
	city_null,
	country_null,
	premium_null,
	premium_now byte

	fname_eq,
	country_eq,
	city_eq,
	email_domain,
	status_eq,
	status_neq,
	sname_eq,
	phone_code,
	birth_year db.DataEntry

	birth_lt,
	birth_gt int32

	fname_any,
	interests_any,
	interests_contains,
	city_any []db.DataEntry

	likes_contains []db.IDAcc
}

type QuerySet struct {
	Vals         [Flength][]byte
	ValidVals    ValidVals
	ValsMask     uint64
	WasRunned    uint64
	FieldMap     account.JSONMask
	HasBadVals   bool
	SkipScan     bool
	Limit        int
	CurrentSet   *roaring.Bitmap
	SortedResult []db.IDAcc
}

func NewQuerySet() *QuerySet {
	return &QuerySet{
		ValsMask:     uint64(1) << F_query_id,
		WasRunned:    uint64(1)<<F_query_id | uint64(1)<<F_limit,
		Limit:        -1,
		CurrentSet:   nil,
		SortedResult: nil,
	}
}

func (qs *QuerySet) Close() {
	db.PutBitmap(qs.CurrentSet)
	PutResultSlice(qs.SortedResult)
}

func (qs *QuerySet) Get(F uint) []byte {
	return qs.Vals[F]
}

func (qs *QuerySet) Set(F uint, val []byte) {
	qs.Vals[F] = val
	if val != nil {
		qs.ValsMask |= uint64(1) << F
	} else {
		qs.ValsMask &^= uint64(1) << F
	}
	if F == F_limit {
		limit, err := strconv.Atoi(string(val))
		if err != nil {
			qs.HasBadVals = true
		} else {
			qs.Limit = limit
		}
	}
}

func (qs *QuerySet) SetKVFilter(key, val []byte) bool {
	k, ok := mapQueryKeysFilter[string(key)]
	if ok {
		qs.Set(k, val)
	}
	return ok
}

func (qs *QuerySet) Is(Fs ...uint) bool {
	m := uint64(1) << F_query_id // всегда установлен
	for _, f := range Fs {
		m |= uint64(1) << f
	}
	return qs.ValsMask == m
}

func (qs *QuerySet) Has(Fs ...uint) bool {
	m := uint64(1) << F_query_id // всегда установлен
	for _, f := range Fs {
		m |= uint64(1) << f
	}
	return (qs.ValsMask & m) == m
}

func (qs *QuerySet) HasOne(F uint) bool {
	m := uint64(1) << F_query_id // всегда установлен
	m |= uint64(1) << F
	return (qs.ValsMask & m) == m
}

func (qs *QuerySet) EnableField(m account.JSONMask) {
	qs.FieldMap |= m
}

func (qs *QuerySet) EnableBad() {
	qs.HasBadVals = true
}

func (qs *QuerySet) EnableSkipScan() {
	qs.SkipScan = true
}

func (qs *QuerySet) CannotRun(F uint) bool {
	return qs.HasBadVals || qs.Get(F) == nil
}

func (qs *QuerySet) HasAndCanRun(F uint) bool {
	return qs.HasOne(F) && !(qs.HasBadVals || qs.Get(F) == nil)
}

func (qs *QuerySet) HasOnlyAndCanRun(Fs ...uint) bool {
	return qs.Is(Fs...) && !qs.HasBadVals
}

func (qs *QuerySet) Validate() {
	if qs.Has(F_sex_eq) {
		qs.EnableField(account.M_sex)
		v := qs.Get(F_sex_eq)
		if len(v) == 1 {
			qs.ValidVals.sex_eq = v[0]
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_email_domain) {
		v := string(qs.Get(F_email_domain))
		if vidx, ok := db.SmAcc.Domain.InDictonary(v); ok {
			qs.ValidVals.email_domain = vidx
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_email_lt) {
		v := string(qs.Get(F_email_lt))
		qs.ValidVals.email_lt = v
	}
	if qs.Has(F_email_gt) {
		v := string(qs.Get(F_email_gt))
		qs.ValidVals.email_gt = v
	}
	if qs.Has(F_status_eq) {
		qs.EnableField(account.M_status)
		v := string(qs.Get(F_status_eq))
		if vidx, ok := db.SmAcc.Status.InDictonary(v); ok {
			qs.ValidVals.status_eq = vidx
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_status_neq) {
		qs.EnableField(account.M_status)
		v := string(qs.Get(F_status_neq))
		if vidx, ok := db.SmAcc.Status.InDictonary(v); ok {
			qs.ValidVals.status_neq = vidx
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_fname_eq) {
		qs.EnableField(account.M_fname)
		v := string(qs.Get(F_fname_eq))
		if vidx, ok := db.SmAcc.FirstName.InDictonary(v); ok {
			qs.ValidVals.fname_eq = vidx
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_fname_any) {
		qs.EnableField(account.M_fname)
		v := string(qs.Get(F_fname_any))
		names := strings.Split(v, ",")
		qs.ValidVals.fname_any = make([]db.DataEntry, 0, len(names))
		for _, nm := range names {
			nms := strings.TrimSpace(nm)
			if vidx, ok := db.SmAcc.FirstName.InDictonary(nms); ok {
				qs.ValidVals.fname_any = append(qs.ValidVals.fname_any, vidx)
			} else {
				qs.EnableBad()
				break
			}
		}
	}
	if qs.Has(F_fname_null) {
		v := qs.Get(F_fname_null)
		if len(v) == 1 {
			if v[0] == '0' {
				qs.EnableField(account.M_fname)
			}
			qs.ValidVals.fname_null = v[0]
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_sname_eq) {
		qs.EnableField(account.M_sname)
		v := string(qs.Get(F_sname_eq))
		if vidx, ok := db.SmAcc.SecondName.InDictonary(v); ok {
			qs.ValidVals.sname_eq = vidx
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_sname_starts) {
		qs.EnableField(account.M_sname)
		v := string(qs.Get(F_sname_starts))
		qs.ValidVals.sname_starts = v
	}
	if qs.Has(F_sname_null) {
		v := qs.Get(F_sname_null)
		if len(v) == 1 {
			if v[0] == '0' {
				qs.EnableField(account.M_sname)
			}
			qs.ValidVals.sname_null = v[0]
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_phone_code) {
		qs.EnableField(account.M_phone)
		v := string(qs.Get(F_phone_code))
		phc, ok := db.SmAcc.PhoneCode.InDictonary(v)
		if ok {
			qs.ValidVals.phone_code = phc
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_phone_null) {
		v := qs.Get(F_phone_null)
		if len(v) == 1 {
			if v[0] == '0' {
				qs.EnableField(account.M_phone)
			}
			qs.ValidVals.phone_null = v[0]
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_country_eq) {
		qs.EnableField(account.M_country)
		v := string(qs.Get(F_country_eq))
		if vidx, ok := db.SmAcc.Country.InDictonary(v); ok {
			qs.ValidVals.country_eq = vidx
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_country_null) {
		v := qs.Get(F_country_null)
		if len(v) == 1 {
			if v[0] == '0' {
				qs.EnableField(account.M_country)
			}
			qs.ValidVals.country_null = v[0]
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_city_eq) {
		qs.EnableField(account.M_city)
		v := string(qs.Get(F_city_eq))
		if vidx, ok := db.SmAcc.City.InDictonary(v); ok {
			qs.ValidVals.city_eq = vidx
		} else {
			// хак ошибки оргов
			qs.EnableSkipScan()
		}
	}
	if qs.Has(F_city_any) {
		qs.EnableField(account.M_city)
		v := string(qs.Get(F_city_any))
		names := strings.Split(v, ",")
		qs.ValidVals.city_any = make([]db.DataEntry, 0, len(names))
		for _, nm := range names {
			nms := strings.TrimSpace(nm)
			if vidx, ok := db.SmAcc.City.InDictonary(nms); ok {
				qs.ValidVals.city_any = append(qs.ValidVals.city_any, vidx)
			}
			// else {
			// хак ошибки оргов
			// }
		}
	}
	if qs.Has(F_city_null) {
		v := qs.Get(F_city_null)
		if len(v) == 1 {
			if v[0] == '0' {
				qs.EnableField(account.M_city)
			}
			qs.ValidVals.city_null = v[0]
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_birth_year) {
		qs.EnableField(account.M_birth)
		v := string(qs.Get(F_birth_year))
		yy, err := strconv.Atoi(v)
		if err != nil {
			qs.EnableBad()
		} else {
			qs.ValidVals.birth_year = db.DataEntry(yy)
		}
	}
	if qs.Has(F_birth_lt) {
		qs.EnableField(account.M_birth)
		v := qs.Get(F_birth_lt)
		qs.ValidVals.birth_lt = db.TimeStamp(v).Int()
	}
	if qs.Has(F_birth_gt) {
		qs.EnableField(account.M_birth)
		v := qs.Get(F_birth_gt)
		qs.ValidVals.birth_gt = db.TimeStamp(v).Int()
	}
	if qs.Has(F_premium_now) {
		v := qs.Get(F_premium_now)
		if len(v) == 1 {
			if v[0] == '1' {
				qs.EnableField(account.M_premium)
			}
			qs.ValidVals.premium_now = v[0]
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_premium_null) {
		v := qs.Get(F_premium_null)
		if len(v) == 1 {
			if v[0] == '0' {
				qs.EnableField(account.M_premium)
			}
			qs.ValidVals.premium_null = v[0]
		} else {
			qs.EnableBad()
		}
	}
	if qs.Has(F_interests_any) {
		v := string(qs.Get(F_interests_any))
		names := strings.Split(v, ",")
		qs.ValidVals.interests_any = make([]db.DataEntry, 0, len(names))
		for _, nm := range names {
			nms := strings.TrimSpace(nm)
			if vidx, ok := db.SmAcc.Interests.InDictonary(nms); ok {
				qs.ValidVals.interests_any = append(qs.ValidVals.interests_any, vidx)
			} else {
				qs.EnableBad()
				break
			}
		}
	}
	if qs.Has(F_interests_contains) {
		v := string(qs.Get(F_interests_contains))
		names := strings.Split(v, ",")
		qs.ValidVals.interests_contains = make([]db.DataEntry, 0, len(names))
		for _, nm := range names {
			nms := strings.TrimSpace(nm)
			if vidx, ok := db.SmAcc.Interests.InDictonary(nms); ok {
				qs.ValidVals.interests_contains = append(qs.ValidVals.interests_contains, vidx)
			} else {
				qs.EnableBad()
				break
			}
		}
	}
	if qs.Has(F_likes_contains) {
		v := string(qs.Get(F_likes_contains))
		names := strings.Split(v, ",")
		qs.ValidVals.likes_contains = make([]db.IDAcc, 0, len(names))
		for _, nm := range names {
			nms := db.IDAccount(strings.TrimSpace(nm)).Int()
			if db.SmAcc.Contains(nms) {
				qs.ValidVals.likes_contains = append(qs.ValidVals.likes_contains, nms)
			} else {
				qs.EnableBad()
				break
			}
		}
	}
}

var resPool = sync.Pool{}

func GetResultSlice() []db.IDAcc {
	sl := resPool.Get()
	if sl != nil {
		vsl := sl.([]db.IDAcc)
		return vsl
	}
	return make([]db.IDAcc, 0, 40) // max limit
}

func PutResultSlice(sl []db.IDAcc) {
	if sl == nil {
		return
	}
	if len(sl) > 100 {
		return
	}
	resPool.Put(sl[:0])
}

func OneScan(qs *QuerySet) {

	if qs.SkipScan {
		return
	}

	limit := qs.Limit
	qs.SortedResult = GetResultSlice()

	if qs.HasOnlyAndCanRun(F_limit) {
		FilterLimit(qs)
		return
	}

	B_country_eq := qs.HasAndCanRun(F_country_eq)
	B_country_null := qs.HasAndCanRun(F_country_null)
	B_city_eq := qs.HasAndCanRun(F_city_eq)
	B_sex_eq := qs.HasAndCanRun(F_sex_eq)
	B_birth_year := qs.HasAndCanRun(F_birth_year)
	B_email_domain := qs.HasAndCanRun(F_email_domain)
	B_email_lt := qs.HasAndCanRun(F_email_lt)
	B_email_gt := qs.HasAndCanRun(F_email_gt)
	B_status_eq := qs.HasAndCanRun(F_status_eq)
	B_status_neq := qs.HasAndCanRun(F_status_neq)
	B_fname_eq := qs.HasAndCanRun(F_fname_eq)
	B_fname_any := qs.HasAndCanRun(F_fname_any)
	B_fname_null := qs.HasAndCanRun(F_fname_null)
	B_sname_eq := qs.HasAndCanRun(F_sname_eq)
	B_sname_starts := qs.HasAndCanRun(F_sname_starts)
	B_sname_null := qs.HasAndCanRun(F_sname_null)
	B_phone_code := qs.HasAndCanRun(F_phone_code)
	B_phone_null := qs.HasAndCanRun(F_phone_null)
	B_city_any := qs.HasAndCanRun(F_city_any)
	B_city_null := qs.HasAndCanRun(F_city_null)
	B_birth_lt := qs.HasAndCanRun(F_birth_lt)
	B_birth_gt := qs.HasAndCanRun(F_birth_gt)
	B_interests_contains := qs.HasAndCanRun(F_interests_contains)
	B_interests_any := qs.HasAndCanRun(F_interests_any)
	B_likes_contains := qs.HasAndCanRun(F_likes_contains)
	B_premium_now := qs.HasAndCanRun(F_premium_now)
	B_premium_null := qs.HasAndCanRun(F_premium_null)

	iter := db.NewIteratorIntersect(true)

	if B_likes_contains {

		bm := db.GetBitmap()
		bmv := db.GetBitmap()
		for i, liked := range qs.ValidVals.likes_contains {
			bmv.Clear()
			ids := db.SmAcc.LikedDict(liked)
			for _, v := range ids {
				bmv.Add(uint32(db.SmAcc.GetLikeLiker(v)))
			}
			if i == 0 {
				bm.Or(bmv)
			} else {
				bm.And(bmv)
			}
		}
		db.PutBitmap(bmv)
		lenres := bm.GetCardinality()
		flt := make([]db.IDAcc, lenres)
		resiter := bm.Iterator()
		i := 0
		for resiter.HasNext() {
			flt[i] = db.IDAcc(resiter.Next())
			i++ // дальше в фильтр передается по возрастанию
		}
		db.PutBitmap(bm)

		iter.Append(db.IteratorByIds(flt, true))
	}

	if B_fname_null {
		if qs.ValidVals.fname_null == '0' {
			iter.Append(db.SmAcc.FirstName.IteratorWithFilterVal(db.SmAcc.FirstName.ZeroVal(), true, true))
		} else if qs.ValidVals.fname_null == '1' {
			iter.Append(db.SmAcc.FirstName.IteratorWithFilterVal(db.SmAcc.FirstName.ZeroVal(), true, false))
		} else {
			qs.EnableBad()
			return
		}
	}

	if B_status_eq {
		if B_city_eq || B_country_eq {
			if B_city_eq {
				iter.Append(db.IteratorByIds(db.SmAcc.StatusCity.GetV(qs.ValidVals.city_eq<<2|qs.ValidVals.status_eq), true))
			}
			if B_country_eq {
				iter.Append(db.IteratorByIds(db.SmAcc.StatusCountry.GetV(qs.ValidVals.country_eq<<2|qs.ValidVals.status_eq), true))
			}
		} else {
			iter.Append(db.SmAcc.Status.IteratorWithFilterVal(qs.ValidVals.status_eq, true, false))
		}
	}

	var sex db.DataEntry
	checkSex := false
	if B_sex_eq {
		if qs.ValidVals.sex_eq == 'm' {
			sex = db.DataEntry(1)
		} else if qs.ValidVals.sex_eq == 'f' {
			sex = db.DataEntry(0)
		} else {
			qs.EnableBad()
			return
		}
		if B_city_eq || B_country_eq || B_city_null || B_country_null {
			if B_city_eq {
				iter.Append(db.IteratorByIds(db.SmAcc.SexCity.GetV(qs.ValidVals.city_eq<<1|sex), true))
			}
			if B_country_eq {
				iter.Append(db.IteratorByIds(db.SmAcc.SexCountry.GetV(qs.ValidVals.country_eq<<1|sex), true))
			}
			if B_city_null {
				if qs.ValidVals.city_null == '0' {
					iter.Append(db.SmAcc.City.IteratorWithFilterVal(db.SmAcc.City.ZeroVal(), true, true))
					checkSex = true
				} else if qs.ValidVals.city_null == '1' {
					iter.Append(db.SmAcc.SexCity.IteratorWithFilterVal(db.SmAcc.City.ZeroVal()<<1|sex, true, false))
				} else {
					qs.EnableBad()
					return
				}
			}
			if B_country_null {
				if qs.ValidVals.country_null == '0' {
					iter.Append(db.SmAcc.Country.IteratorWithFilterVal(db.SmAcc.Country.ZeroVal(), true, true))
					checkSex = true
				} else if qs.ValidVals.country_null == '1' {
					iter.Append(db.SmAcc.SexCountry.IteratorWithFilterVal(db.SmAcc.Country.ZeroVal()<<1|sex, true, false))
				} else {
					qs.EnableBad()
					return
				}
			}
		} else {
			checkSex = true
		}
	}

	if B_country_null && !B_sex_eq {
		if qs.ValidVals.country_null == '0' {
			iter.Append(db.SmAcc.Country.IteratorWithFilterVal(db.SmAcc.Country.ZeroVal(), true, true))
		} else if qs.ValidVals.country_null == '1' {
			iter.Append(db.SmAcc.Country.IteratorWithFilterVal(db.SmAcc.Country.ZeroVal(), true, false))
		} else {
			qs.EnableBad()
			return
		}
	}

	if B_city_null && !B_sex_eq {
		if qs.ValidVals.city_null == '0' {
			iter.Append(db.SmAcc.City.IteratorWithFilterVal(db.SmAcc.City.ZeroVal(), true, true))
		} else if qs.ValidVals.city_null == '1' {
			iter.Append(db.SmAcc.City.IteratorWithFilterVal(db.SmAcc.City.ZeroVal(), true, false))
		} else {
			qs.EnableBad()
		}
	}

	if B_sname_null {
		if qs.ValidVals.sname_null == '0' {
			iter.Append(db.SmAcc.SecondName.IteratorWithFilterVal(db.SmAcc.SecondName.ZeroVal(), true, true))
		} else if qs.ValidVals.sname_null == '1' {
			iter.Append(db.SmAcc.SecondName.IteratorWithFilterVal(db.SmAcc.SecondName.ZeroVal(), true, false))
		} else {
			qs.EnableBad()
		}
	}

	if B_phone_null {
		if qs.ValidVals.phone_null == '0' {
			iter.Append(db.SmAcc.PhoneCode.IteratorWithFilterVal(db.SmAcc.PhoneCode.ZeroVal(), true, true))
		} else if qs.ValidVals.phone_null == '1' {
			iter.Append(db.SmAcc.PhoneCode.IteratorWithFilterVal(db.SmAcc.PhoneCode.ZeroVal(), true, false))
		} else {
			qs.EnableBad()
		}
	}

	if B_city_eq && !B_status_eq && !B_sex_eq {
		iter.Append(db.SmAcc.City.IteratorWithFilterVal(qs.ValidVals.city_eq, true, false))
	}

	if B_fname_eq {
		iter.Append(db.SmAcc.FirstName.IteratorWithFilterVal(qs.ValidVals.fname_eq, true, false))
	}

	if B_sname_eq {
		iter.Append(db.SmAcc.SecondName.IteratorWithFilterVal(qs.ValidVals.sname_eq, true, false))
	}

	if B_phone_code {
		iter.Append(db.SmAcc.PhoneCode.IteratorWithFilterVal(qs.ValidVals.phone_code, true, false))
	}

	if B_country_eq && !B_status_eq && !B_sex_eq {
		iter.Append(db.SmAcc.Country.IteratorWithFilterVal(qs.ValidVals.country_eq, true, false))
	}

	if B_city_any {
		merge_iters := make([]db.IDIterator, len(qs.ValidVals.city_any))
		for i, dv := range qs.ValidVals.city_any {
			merge_iters[i] = db.SmAcc.City.IteratorWithFilterVal(dv, true, false)
		}
		mrgiter := db.NewMergeIterator(merge_iters...)
		iter.Append(mrgiter)
	}

	if B_fname_any {
		merge_iters := make([]db.IDIterator, len(qs.ValidVals.fname_any))
		for i, dv := range qs.ValidVals.fname_any {
			merge_iters[i] = db.SmAcc.FirstName.IteratorWithFilterVal(dv, true, false)
		}
		mrgiter := db.NewMergeIterator(merge_iters...)
		iter.Append(mrgiter)
	}

	if B_interests_contains {
		for _, v := range qs.ValidVals.interests_contains {
			iter.Append(db.SmAcc.Interests.IteratorWithFilterVal(v, true, false))
		}
	}

	if B_interests_any {
		merge_iters := make([]db.IDIterator, len(qs.ValidVals.interests_any))
		for i, dv := range qs.ValidVals.interests_any {
			merge_iters[i] = db.SmAcc.Interests.IteratorWithFilterVal(dv, true, false)
		}
		mrgiter := db.NewMergeIterator(merge_iters...)
		iter.Append(mrgiter)
	}

	if B_email_domain {
		iter.Append(db.SmAcc.Domain.IteratorWithFilterVal(qs.ValidVals.email_domain, true, false))
	}

	if B_birth_year {
		iter.Append(db.SmAcc.BirthYear.IteratorWithFilterVal(qs.ValidVals.birth_year, true, false))
	}

	if B_status_neq {
		if iter.Size() == 0 {
			iter.Append(db.SmAcc.Status.IteratorWithFilterVal(qs.ValidVals.status_neq, true, true))
		} else {
			iter.AppendDiff(db.SmAcc.Status.IteratorWithFilterVal(qs.ValidVals.status_neq, true, false))
		}
	}

	var mainiter db.IDIterator
	if (iter.Size() + iter.SizeDiffs()) == 0 {
		mainiter = db.SmAcc.Iterator()
	} else if iter.Size() == 1 && iter.SizeDiffs() == 0 {
		mainiter = iter.Iter(0)
	} else {
		mainiter = iter
	}

mainloop:
	for mainiter.HasNext() {
		if limit <= 0 || qs.HasBadVals {
			break
		}

		acc, ok := db.SmAcc.GetById(mainiter.NextID())
		if !ok {
			qs.EnableBad()
			break
		}

		if checkSex && acc.Sex != qs.ValidVals.sex_eq {
			continue
		}

		if B_email_lt {
			eml := dict.DictonaryEml.Get(uint32(acc.ID))
			if strings.Compare(eml, qs.ValidVals.email_lt) >= 0 {
				continue mainloop
			}
		}

		if B_email_gt {
			eml := dict.DictonaryEml.Get(uint32(acc.ID))
			if strings.Compare(eml, qs.ValidVals.email_gt) <= 0 {
				continue mainloop
			}
		}

		if B_sname_starts {
			s := db.SmAcc.SecondName.GetString(acc.ID)
			if !strings.HasPrefix(s, qs.ValidVals.sname_starts) {
				continue mainloop
			}
		}

		if B_birth_lt {
			if acc.Birth >= qs.ValidVals.birth_lt {
				continue mainloop
			}
		}

		if B_birth_gt {
			if acc.Birth <= qs.ValidVals.birth_gt {
				continue mainloop
			}
		}

		if B_premium_now {
			if qs.ValidVals.premium_now == '1' {
				if (acc.PremiumStart == db.NullTime || acc.PremiumFinish == db.NullTime) ||
					(acc.PremiumStart > db.CurrentTime) || (db.CurrentTime > acc.PremiumFinish) {
					continue mainloop
				}
			} else if qs.ValidVals.premium_now == '0' {
				if acc.PremiumStart != db.NullTime && acc.PremiumFinish != db.NullTime &&
					acc.PremiumStart <= db.CurrentTime && db.CurrentTime <= acc.PremiumFinish {
					continue mainloop
				}
			} else {
				qs.EnableBad()
			}
		}

		if B_premium_null {
			if qs.ValidVals.premium_null == '0' {
				if acc.PremiumStart == db.NullTime || acc.PremiumFinish == db.NullTime {
					continue mainloop
				}
			} else if qs.ValidVals.premium_null == '1' {
				if !(acc.PremiumStart == db.NullTime && acc.PremiumFinish == db.NullTime) {
					continue mainloop
				}
			} else {
				qs.EnableBad()
			}
		}

		qs.SortedResult = append(qs.SortedResult, acc.ID)
		limit--
	}
}
