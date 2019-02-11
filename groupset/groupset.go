package groupset

import (
	"bytes"
	"github.com/covrom/highloadcup2018/account"
	"github.com/covrom/highloadcup2018/db"
	"github.com/covrom/highloadcup2018/dict"
	"io"
	"strconv"
)

type GroupResult struct {
	Sex byte
	City,
	Status,
	Interests,
	Country db.DataEntry

	Count uint32
}

type ValidVals struct {
	sex byte

	email uint32

	phone string

	city,
	status,
	country,
	birth_year,
	interests,
	fname,
	sname,
	joined_year db.DataEntry

	likes db.IDAcc
}

type GroupSet struct {
	Vals         [Flength][]byte
	ValidVals    ValidVals
	FilterMask   uint32
	KeysMask     uint8
	KeysOrder    [glength]int8
	HasBadVals   bool
	Limit        int8
	Order        int8
	SortedResult []GroupResult
}

func NewGroupSet() *GroupSet {
	return &GroupSet{
		FilterMask:   uint32(1) << F_query_id,
		Limit:        -1,
		SortedResult: nil,
	}
}

func (gs *GroupSet) Close() {
}

func (gs *GroupSet) Get(F uint) []byte {
	return gs.Vals[F]
}

func (gs *GroupSet) Set(F uint, val []byte) {
	gs.Vals[F] = val
	if val != nil {
		gs.FilterMask |= uint32(1) << F
	} else {
		gs.FilterMask &^= uint32(1) << F
	}
	if F == F_limit {
		limit, err := strconv.Atoi(string(val))
		if err != nil {
			gs.HasBadVals = true
		} else {
			gs.Limit = int8(limit)
		}
	} else if F == F_order {
		order, err := strconv.Atoi(string(val))
		if err == nil && (order == -1 || order == 1) {
			gs.Order = int8(order)
		} else {
			gs.HasBadVals = true
		}
	} else if F == F_keys {
		keys := bytes.Split(val, []byte(","))
		for ikey, key := range keys {
			k, ok := mapQueryKeys[string(key)]
			if ok {
				gs.KeysMask |= uint8(1) << k
				gs.KeysOrder[k] = int8(ikey)
			} else {
				gs.HasBadVals = true
				break
			}
		}
	}
}

func (gs *GroupSet) SetKVFilter(key, val []byte) bool {
	k, ok := mapQueryFilter[string(key)]
	if ok {
		gs.Set(k, val)
	}
	return ok
}

func (gs *GroupSet) Is(Fs ...uint) bool {
	m := uint32(1) << F_query_id // всегда установлен
	for _, f := range Fs {
		m |= uint32(1) << f
	}
	return gs.FilterMask == m
}

func (gs *GroupSet) Has(Fs ...uint) bool {
	m := uint32(1) << F_query_id // всегда установлен
	for _, f := range Fs {
		m |= uint32(1) << f
	}
	return (gs.FilterMask & m) == m
}

func (gs *GroupSet) HasOne(F uint) bool {
	m := uint32(1) << F
	return (gs.FilterMask & m) == m
}

func (gs *GroupSet) HasKey(G GroupKey) bool {
	m := uint8(1) << G // всегда установлен
	return (gs.KeysMask & m) == m
}

func (gs *GroupSet) EnableBad() {
	gs.HasBadVals = true
}

func (gs *GroupSet) CannotRun(F uint) bool {
	return gs.HasBadVals || gs.Get(F) == nil
}

func (gs *GroupSet) HasAndCanRun(F uint) bool {
	return gs.HasOne(F) && !(gs.HasBadVals || gs.Get(F) == nil)
}

func (gs *GroupSet) HasOnlyAndCanRun(Fs ...uint) bool {
	return gs.Is(Fs...) && !gs.HasBadVals
}

func (gs *GroupSet) MarshalResult(w io.Writer) {
	jw := account.Writer{Buffer: w}
	jw.RawString(`{"groups":[`)
	// проходим в нужном порядке через пул воркеров
	for i, res := range gs.SortedResult {
		if gs.Limit >= 0 && int8(i) >= gs.Limit {
			break
		}
		if i > 0 {
			jw.RawByte(',')
		}
		jw.RawByte('{')
		ncm := false
		if gs.HasKey(G_sex) {
			if res.Sex != 0 {
				if ncm {
					jw.RawByte(',')
				}
				jw.RawString(`"sex":"`)
				jw.RawByte(res.Sex)
				jw.RawString(`"`)
				ncm = true
			}
		}
		if gs.HasKey(G_status) {
			if !db.SmAcc.Status.IsZero(res.Status) {
				if ncm {
					jw.RawByte(',')
				}
				jw.RawString(`"status":`)
				jw.String(db.SmAcc.Status.FromDictonary(res.Status))
				ncm = true
			}
		}
		if gs.HasKey(G_interests) {
			if !db.SmAcc.Status.IsZero(res.Interests) {
				if ncm {
					jw.RawByte(',')
				}
				jw.RawString(`"interests":`)
				jw.String(db.SmAcc.Interests.FromDictonary(res.Interests))
				ncm = true
			}
		}
		if gs.HasKey(G_city) {
			if !db.SmAcc.City.IsZero(res.City) {
				if ncm {
					jw.RawByte(',')
				}
				jw.RawString(`"city":`)
				jw.String(db.SmAcc.City.FromDictonary(res.City))
				ncm = true
			}
		}
		if gs.HasKey(G_country) {
			if !db.SmAcc.Country.IsZero(res.Country) {
				if ncm {
					jw.RawByte(',')
				}
				jw.RawString(`"country":`)
				jw.String(db.SmAcc.Country.FromDictonary(res.Country))
				ncm = true
			}
		}
		if ncm {
			jw.RawByte(',')
		}
		jw.RawString(`"count":`)
		jw.Uint32(res.Count)

		jw.RawByte('}')
	}
	jw.RawString(`]}`)
}

func (gs *GroupSet) Validate() {
	if gs.Has(F_sex) {
		v := gs.Get(F_sex)
		if len(v) == 1 && (v[0] == 'm' || v[0] == 'f') {
			gs.ValidVals.sex = v[0]
		} else {
			gs.EnableBad()
		}
	}
	if gs.Has(F_email) {
		v := string(gs.Get(F_email))
		if vidx, ok := dict.DictonaryEml.In(v); ok {
			gs.ValidVals.email = vidx
		} else {
			gs.EnableBad()
		}
	}
	if gs.Has(F_status) {
		v := string(gs.Get(F_status))
		if vidx, ok := db.SmAcc.Status.InDictonary(v); ok {
			gs.ValidVals.status = vidx
		} else {
			gs.EnableBad()
		}
	}
	if gs.Has(F_fname) {
		v := string(gs.Get(F_fname))
		if vidx, ok := db.SmAcc.FirstName.InDictonary(v); ok {
			gs.ValidVals.fname = vidx
		} else {
			gs.EnableBad()
		}
	}
	if gs.Has(F_sname) {
		v := string(gs.Get(F_sname))
		if vidx, ok := db.SmAcc.SecondName.InDictonary(v); ok {
			gs.ValidVals.sname = vidx
		} else {
			gs.EnableBad()
		}
	}
	if gs.Has(F_phone) {
		v := string(gs.Get(F_phone))
		gs.ValidVals.phone = v
	}
	if gs.Has(F_country) {
		v := string(gs.Get(F_country))
		if vidx, ok := db.SmAcc.Country.InDictonary(v); ok {
			gs.ValidVals.country = vidx
		} else {
			gs.EnableBad()
		}
	}
	if gs.Has(F_city) {
		v := string(gs.Get(F_city))
		if vidx, ok := db.SmAcc.City.InDictonary(v); ok {
			gs.ValidVals.city = vidx
		} else {
			gs.EnableBad()
		}
	}
	if gs.Has(F_birth) {
		v := string(gs.Get(F_birth))
		yy, err := strconv.Atoi(v)
		if err != nil {
			gs.EnableBad()
		} else {
			gs.ValidVals.birth_year = db.DataEntry(yy)
		}
	}
	if gs.Has(F_joined) {
		v := string(gs.Get(F_joined))
		yy, err := strconv.Atoi(v)
		if err != nil {
			gs.EnableBad()
		} else {
			gs.ValidVals.joined_year = db.DataEntry(yy)
		}
	}
	if gs.Has(F_interests) {
		v := string(gs.Get(F_interests))
		if vidx, ok := db.SmAcc.Interests.InDictonary(v); ok {
			gs.ValidVals.interests = vidx
		} else {
			gs.EnableBad()
		}
	}
	if gs.Has(F_likes) {
		v := gs.Get(F_likes)
		nms := db.IDAccount(v).Int()
		if db.SmAcc.Contains(nms) {
			gs.ValidVals.likes = nms
		} else {
			gs.EnableBad()
		}
	}
}

func OneScan(gs *GroupSet) {

	// не участвуют в рейтинговом датасете: email, fname, sname, phone, premium
	// sex, country, city, status, interests есть и в отборах и в группах
	// birth, joined год в отборах
	// likes в отборах

	B_sex := gs.HasAndCanRun(F_sex)
	B_country := gs.HasAndCanRun(F_country)
	B_city := gs.HasAndCanRun(F_city)
	B_status := gs.HasAndCanRun(F_status)
	B_interests := gs.HasAndCanRun(F_interests)
	B_birth_year := gs.HasAndCanRun(F_birth)
	B_joined_year := gs.HasAndCanRun(F_joined)
	B_likes := gs.HasAndCanRun(F_likes)

	K_sex := gs.HasKey(G_sex)
	K_country := gs.HasKey(G_country)
	K_city := gs.HasKey(G_city)
	K_status := gs.HasKey(G_status)
	K_interests := gs.HasKey(G_interests)

	var cnt_sex,
		cnt_country,
		cnt_city,
		cnt_country_sex,
		cnt_status,
		cnt_inter,
		cnt_city_sex,
		cnt_status_city,
		cnt_status_country []uint32

	switch {
	case K_sex && K_country:
		cnt_country_sex = make([]uint32, db.SmAcc.Country.DictCardinality()*2)
	case K_sex && K_city:
		cnt_city_sex = make([]uint32, db.SmAcc.City.DictCardinality()*2)
	case K_status && K_city:
		cnt_status_city = make([]uint32, db.SmAcc.City.DictCardinality()*4)
	case K_status && K_country:
		cnt_status_country = make([]uint32, db.SmAcc.Country.DictCardinality()*4)
	case K_sex:
		cnt_sex = make([]uint32, 2)
	case K_country:
		cnt_country = make([]uint32, db.SmAcc.Country.DictCardinality())
	case K_city:
		cnt_city = make([]uint32, db.SmAcc.City.DictCardinality())
	case K_status:
		cnt_status = make([]uint32, db.SmAcc.Status.DictCardinality())
	case K_interests:
		cnt_inter = make([]uint32, db.SmAcc.Interests.DictCardinality())
	}

	iter := db.NewIteratorIntersect(false)

	if B_status {
		if B_city || B_country {
			if B_city {
				iter.Append(db.IteratorByIds(db.SmAcc.StatusCity.GetV(gs.ValidVals.city<<2|gs.ValidVals.status), false))
			}
			if B_country {
				iter.Append(db.IteratorByIds(db.SmAcc.StatusCountry.GetV(gs.ValidVals.country<<2|gs.ValidVals.status), false))
			}
		} else {
			iter.Append(db.SmAcc.Status.IteratorWithFilterVal(gs.ValidVals.status, false, false))
		}
	}

	var sex db.DataEntry
	if B_sex {
		if gs.ValidVals.sex == 'm' {
			sex = db.DataEntry(1)
		} else if gs.ValidVals.sex == 'f' {
			sex = db.DataEntry(0)
		} else {
			gs.EnableBad()
			return
		}
		if B_city || B_country {
			if B_city {
				iter.Append(db.IteratorByIds(db.SmAcc.SexCity.GetV(gs.ValidVals.city<<1|sex), false))
			}
			if B_country {
				iter.Append(db.IteratorByIds(db.SmAcc.SexCountry.GetV(gs.ValidVals.country<<1|sex), false))
			}
		} else {
			iter.Append(db.SmAcc.Sex.IteratorWithFilterVal(sex, false, false))
		}
	}

	if B_country && !B_status && !B_sex {
		iter.Append(db.SmAcc.Country.IteratorWithFilterVal(gs.ValidVals.country, false, false))
	}

	if B_city && !B_status && !B_sex {
		iter.Append(db.SmAcc.City.IteratorWithFilterVal(gs.ValidVals.city, false, false))
	}

	if B_birth_year {
		iter.Append(db.SmAcc.BirthYear.IteratorWithFilterVal(gs.ValidVals.birth_year, false, false))
	}

	if B_joined_year {
		iter.Append(db.SmAcc.JoinedYear.IteratorWithFilterVal(gs.ValidVals.joined_year, false, false))
	}

	if B_interests {
		iter.Append(db.SmAcc.Interests.IteratorWithFilterVal(gs.ValidVals.interests, false, false))
	}

	if B_likes {
		lks := db.SmAcc.LikedDict(gs.ValidVals.likes)
		if len(lks) > 0 {
			bmv := db.GetBitmap()
			for _, v := range lks {
				bmv.Add(uint32(db.SmAcc.GetLikeLiker(v)))
			}
			lenres := bmv.GetCardinality()
			flt := make([]db.IDAcc, lenres)
			resiter := bmv.Iterator()
			i := 0
			for resiter.HasNext() {
				flt[i] = db.IDAcc(resiter.Next())
				i++ // дальше в фильтр передается по возрастанию
			}
			db.PutBitmap(bmv)
			iter.Append(db.IteratorByIds(flt, false))
		}
	}

	switch {
	case iter.Size() == 0:
		// без фильтров
		switch {
		case K_sex && K_country:
			db.SmAcc.SexCountry.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
				cnt_country_sex[v] += uint32(len(ids))
			})
		case K_sex && K_city:
			db.SmAcc.SexCity.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
				cnt_city_sex[v] += uint32(len(ids))
			})
		case K_status && K_city:
			db.SmAcc.StatusCity.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
				cnt_status_city[v] += uint32(len(ids))
			})
		case K_status && K_country:
			db.SmAcc.StatusCountry.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
				cnt_status_country[v] += uint32(len(ids))
			})
		case K_sex:
			cnt_sex[0] = uint32(db.SmAcc.Sex.GetCountV(0))
			cnt_sex[1] = uint32(db.SmAcc.Sex.GetCountV(1))
		case K_country:
			db.SmAcc.Country.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
				cnt_country[v] += uint32(len(ids))
			})
		case K_city:
			db.SmAcc.City.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
				cnt_city[v] += uint32(len(ids))
			})
		case K_status:
			cnt_status[0] = uint32(db.SmAcc.Status.GetCountV(0))
			cnt_status[1] = uint32(db.SmAcc.Status.GetCountV(1))
			cnt_status[2] = uint32(db.SmAcc.Status.GetCountV(2))
			cnt_status[3] = uint32(db.SmAcc.Status.GetCountV(3))
		case K_interests:
			db.SmAcc.Interests.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
				cnt_inter[v] += uint32(len(ids))
			})
		}
	case iter.Size() == 1 && B_status && K_city && !K_sex:
		db.SmAcc.City.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
			if K_status {
				vv := v<<2 | gs.ValidVals.status
				cnt_status_city[vv] += uint32(len(db.SmAcc.StatusCity.GetV(vv)))
			} else {
				cnt_city[v] += uint32(len(db.SmAcc.StatusCity.GetV(v<<2 | gs.ValidVals.status)))
			}
		})
	case iter.Size() == 1 && B_status && K_country && !K_sex:
		db.SmAcc.Country.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
			if K_status {
				vv := v<<2 | gs.ValidVals.status
				cnt_status_country[vv] += uint32(len(db.SmAcc.StatusCountry.GetV(vv)))
			} else {
				cnt_country[v] += uint32(len(db.SmAcc.StatusCountry.GetV(v<<2 | gs.ValidVals.status)))
			}
		})
	case iter.Size() == 1 && B_sex && K_city && !K_status:
		db.SmAcc.City.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
			if K_sex {
				vv := v<<1 | sex
				cnt_city_sex[vv] += uint32(len(db.SmAcc.SexCity.GetV(vv)))
			} else {
				cnt_city[v] += uint32(len(db.SmAcc.SexCity.GetV(v<<1 | sex)))
			}
		})
	case iter.Size() == 1 && B_sex && K_country && !K_status:
		db.SmAcc.Country.RangeVals(func(v db.DataEntry, ids []db.IDAcc) {
			if K_sex {
				vv := v<<1 | sex
				cnt_country_sex[vv] += uint32(len(db.SmAcc.SexCountry.GetV(vv)))
			} else {
				cnt_country[v] += uint32(len(db.SmAcc.SexCountry.GetV(v<<1 | sex)))
			}
		})
	default:
		switch {
		case K_sex && K_country:
			for iter.HasNext() {
				accid := iter.NextID()
				cnt_country_sex[db.SmAcc.SexCountry.Get(accid)]++
			}
		case K_sex && K_city:
			for iter.HasNext() {
				accid := iter.NextID()
				cnt_city_sex[db.SmAcc.SexCity.Get(accid)]++
			}
		case K_status && K_city:
			for iter.HasNext() {
				accid := iter.NextID()
				cnt_status_city[db.SmAcc.StatusCity.Get(accid)]++
			}
		case K_status && K_country:
			for iter.HasNext() {
				accid := iter.NextID()
				cnt_status_country[db.SmAcc.StatusCountry.Get(accid)]++
			}
		case K_sex:
			for iter.HasNext() {
				accid := iter.NextID()
				cnt_sex[db.SmAcc.Sex.Get(accid)]++
			}
		case K_country:
			for iter.HasNext() {
				accid := iter.NextID()
				cnt_country[db.SmAcc.Country.Get(accid)]++
			}
		case K_city:
			for iter.HasNext() {
				accid := iter.NextID()
				cnt_city[db.SmAcc.City.Get(accid)]++
			}
		case K_status:
			for iter.HasNext() {
				accid := iter.NextID()
				cnt_status[db.SmAcc.Status.Get(accid)]++
			}
		case K_interests:
			for iter.HasNext() {
				accid := iter.NextID()
				ints := db.SmAcc.Interests.GetSet(accid)
				for _, v := range ints {
					cnt_inter[v]++
				}
			}
		}
	}

	stats := NewGroupStats(gs)

	switch {
	case K_sex && K_country:
		for i, v := range cnt_country_sex {
			if v == 0 {
				continue
			}
			var sex byte
			if i%2 == 0 {
				sex = 'f'
			} else {
				sex = 'm'
			}
			country := db.DataEntry(i >> 1)

			stats.Update(stats.fwd_sex_country(sex, country, v),
				stats.rev_sex_country(sex, country, v),
				GroupResult{
					Country: country,
					Sex:     sex,
					Count:   v,
				})
		}
	case K_sex && K_city:
		for i, v := range cnt_city_sex {
			if v == 0 {
				continue
			}
			var sex byte
			if i%2 == 0 {
				sex = 'f'
			} else {
				sex = 'm'
			}
			city := db.DataEntry(i >> 1)

			stats.Update(stats.fwd_sex_city(sex, city, v),
				stats.rev_sex_city(sex, city, v),
				GroupResult{
					City:  city,
					Sex:   sex,
					Count: v,
				})
		}
	case K_status && K_city:
		for i, v := range cnt_status_city {
			if v == 0 {
				continue
			}
			status := db.DataEntry(i & 3)
			city := db.DataEntry(i >> 2)

			stats.Update(stats.fwd_status_city(status, city, v),
				stats.rev_status_city(status, city, v),
				GroupResult{
					City:   city,
					Status: status,
					Count:  v,
				})
		}
	case K_status && K_country:
		for i, v := range cnt_status_country {
			if v == 0 {
				continue
			}
			status := db.DataEntry(i & 3)
			country := db.DataEntry(i >> 2)

			stats.Update(stats.fwd_status_country(status, country, v),
				stats.rev_status_country(status, country, v),
				GroupResult{
					Country: country,
					Status:  status,
					Count:   v,
				})
		}
	case K_sex:
		var v uint32
		var sex byte
		v = cnt_sex[0]
		sex = byte('f')
		if v > 0 {
			stats.Update(stats.fwd_sex(sex, v),
				stats.rev_sex(sex, v),
				GroupResult{
					Sex:   sex,
					Count: v,
				})
		}
		v = cnt_sex[1]
		sex = byte('m')
		if v > 0 {
			stats.Update(stats.fwd_sex(sex, v),
				stats.rev_sex(sex, v),
				GroupResult{
					Sex:   sex,
					Count: v,
				})
		}
	case K_country:
		for i, v := range cnt_country {
			if v == 0 {
				continue
			}
			country := db.DataEntry(i)
			stats.Update(stats.fwd_country(country, v),
				stats.rev_country(country, v),
				GroupResult{
					Country: country,
					Count:   v,
				})
		}
	case K_city:
		for i, v := range cnt_city {
			if v == 0 {
				continue
			}
			city := db.DataEntry(i)
			stats.Update(stats.fwd_city(city, v),
				stats.rev_city(city, v),
				GroupResult{
					City:  city,
					Count: v,
				})
		}
	case K_status:
		for i, v := range cnt_status {
			if v == 0 {
				continue
			}
			status := db.DataEntry(i)
			stats.Update(stats.fwd_status(status, v),
				stats.rev_status(status, v),
				GroupResult{
					Status: status,
					Count:  v,
				})
		}
	case K_interests:
		for i, v := range cnt_inter {
			if v == 0 {
				continue
			}
			interest := db.DataEntry(i)
			stats.Update(stats.fwd_interest(interest, v),
				stats.rev_interest(interest, v),
				GroupResult{
					Interests: interest,
					Count:     v,
				})
		}
	}

	gs.SortedResult = stats.Result()
}