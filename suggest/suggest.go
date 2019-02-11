package suggest

import (
	"github.com/covrom/highloadcup2018/db"
	"sort"
	"sync"
)

// Этот тип запросов похож на предыдущий тем, что он тоже про поиск "вторых половинок".
// Аналогично пересылается id пользователя, для которого мы ищем вторую половинку и аналогично используется GET-параметер limit.
// Различия в реализации. Теперь мы ищем, кого лайкают пользователи того же пола с похожими "симпатиями" и предлагаем тех, кого они недавно лайкали сами.
// В случае, если в запросе передан GET-параметр country или city, то искать "похожие симпатии" нужно только в определённой локации.

// Похожесть симпатий определим как функцию: similarity = f (me, account), которая вычисляется однозначно
// как сумма из дробей 1 / abs(my_like['ts'] - like['ts']), где my_like и like - это симпатии к одному и тому же пользователю.
// Для дроби, где my_like['ts'] == like['ts'], заменяем дробь на 1.
// Если общих лайков нет, то стоит считать пользователей абсолютно непохожими с similarity = 0.
// Если у одного аккаунта есть несколько лайков на одного и того же пользователя с разными датами, то в формуле используется среднее арифметическое их дат.

// В ответе возвращается список тех, кого ещё не лайкал пользователь с указанным id, но кого лайкали пользователи с самыми похожими симпатиями.
// Сортировка по убыванию похожести, а между лайками одного такого пользователя - по убыванию id лайка.

// Пример запроса и корректного ответа на него:

// GET: /accounts/51774/suggest/?country=Испляндия&limit=6&query_id=152
// {
//     "accounts": [
//         {
//              "email": "itwonudiahsu@yandex.ru",
//              "id": 94155,
//              "status": "заняты",
//              "fname": "Никита"
//         },{
//              "email": "neeficyreddohypot@ymail.com",
//              "id": 93449,
//              "status": "свободны",
//              "fname": "Иван"
//         },{
//              "email": "sotheralnes@inbox.ru",
//              "id": 89997,
//              "sname": "Лукетатин",
//              "fname": "Руслан",
//              "status": "заняты"
//         },{
//              "email": "kihatneselritunuwryr@ya.ru",
//              "id": 88119,
//              "sname": "Лукушутин",
//              "fname": "Николай",
//              "status": "свободны"
//         },{
//              "email": "otnideonfomedec@icloud.com",
//              "id": 87873,
//              "status": "свободны",
//              "sname": "Фаетавен",
//              "fname": "Сидор"
//         },{
//              "email": "poodreantasis@me.com",
//              "id": 85461,
//              "sname": "Даныкалан",
//              "fname": "Вадим",
//              "status": "заняты"
//         },
//     ]
// }
// Особенность 9. Если в хранимых данных не существует пользователя с переданным id, то ожидается код 404 с пустым телом ответа.

// !!!!!!!!!!!!!!!!!!
// suggest отлично ложится на индексы. 2 индекса одного типа
// Нужно быстро искать юзеров с similarity!=0. (из этого вроде очевидно какие индексы нужны)
// юзеров с similarity!=0 порядка 1000
// да просто индекс по лайкам туда и обратно

// сперва я прохожусь по всем своим лайкам потом я смотрю всех кто этих же юзеров лайкал попутно отсеивая по фильтру
// считаю по формуле similarity и складываю в линейный массив структур {id, similarity}
// потом сортирую этот массив по similarity по убыванию
// дальше я иду по массиву и смотрю кого лайкал айди из элемента массива, если это не я то пишу его в ответ

// Когда указан city/country, получается он ограничивает только тех, кого сортируешь по similarity.
// На конечные рекомендации это ограничение не распространяется
// если несколько лайков совпадает надо складывать 1/dt
// получил я 702 пользователя у которых similarity>0.
// Кого из 702 считать пользователями с наибольшими симпатиями? У кого наибольший similarity у тех, кто его лайкнул, то есть первого максимального.
// Для сортировки подходит min-heap priority list на базе слайса.
// Далее
// - сортируешь этих "вторых" по убыванию similarity,
// - итерируешь их,
// - у каждого итерируешь тех, кого он полайках по убыванию id,

type simrec struct {
	like db.IDAcc
	sim  float64
}

var simPool = sync.Pool{}

func GetSimSlice(c int32) []simrec {
	sl := simPool.Get()
	if sl != nil {
		vsl := sl.([]simrec)
		if cap(vsl) >= int(c) {
			return vsl
		}
	}
	return make([]simrec, 0, c)
}

func PutSimSlice(sl []simrec) {
	if sl == nil {
		return
	}
	simPool.Put(sl[:0])
}

var cndPool = sync.Pool{}

func GetCndsSlice(c uint64) []db.IDAcc {
	sl := cndPool.Get()
	if sl != nil {
		vsl := sl.([]db.IDAcc)
		if cap(vsl) >= int(c) {
			return vsl
		}
	}
	return make([]db.IDAcc, 0, c)
}

func PutCndsSlice(sl []db.IDAcc) {
	if sl == nil {
		return
	}
	cndPool.Put(sl[:0])
}

var cndsmPool = sync.Pool{}

func GetCndsmSlice(c uint64) []db.IDAcc {
	sl := cndsmPool.Get()
	if sl != nil {
		vsl := sl.([]db.IDAcc)
		if cap(vsl) >= int(c) {
			return vsl
		}
	}
	return make([]db.IDAcc, 0, c)
}

func PutCndsmSlice(sl []db.IDAcc) {
	if sl == nil {
		return
	}
	cndsmPool.Put(sl[:0])
}

func OneScan(id db.IDAcc, country, city db.DataEntry, limit int) (res []db.IDAcc) {
	if limit == 0 {
		return
	}

	// берем свои лайки по убыванию, смотрим, кто (сам того же пола) еще лайкал тех же самых
	mylikes := db.SmAcc.Likes(id)
	sort.Sort(db.SortByLikedReverse(mylikes))

	myidsbmp := db.GetBitmap()

	for _, v := range mylikes {
		lk := v.Liked()
		myidsbmp.Add(uint32(lk))
	}

	candidates := db.GetBitmap()
	myids := myidsbmp.Iterator()
	for myids.HasNext() {
		liked := db.IDAcc(myids.Next())
		ids := db.SmAcc.LikedDict(liked)
		for _, v := range ids {
			lkr := db.SmAcc.GetLikeLiker(v)
			if lkr != id {
				candidates.Add(uint32(lkr))
			}
		}
	}

	if candidates.IsEmpty() {
		return
	}

	sex := db.SmAcc.Sex.Get(id)

	var fltiter db.IDIterator

	checkSex := false
	if city > 0 {
		fltiter = db.SmAcc.SexCity.IteratorWithFilterVal(city<<1|sex, false, false)
	} else if country > 0 {
		fltiter = db.SmAcc.SexCountry.IteratorWithFilterVal(country<<1|sex, false, false)
	} else {
		checkSex = true
	}

	candids := GetCndsSlice(candidates.GetCardinality())
	candidsiter := candidates.Iterator()
	for candidsiter.HasNext() {
		candids = append(candids, db.IDAcc(candidsiter.Next()))
	}

	iter := db.NewIteratorIntersect(false)
	iter.Append(db.IteratorByIds(candids, false))
	iter.Append(fltiter)

	sims := GetSimSlice(heapLimit)

	cndids := GetCndsmSlice(20)

	for iter.HasNext() {
		cndID := iter.NextID()
		if checkSex && db.SmAcc.Sex.Get(cndID) != sex {
			continue
		}
		cndlikes := db.SmAcc.LikesDict(cndID)
		cndids = cndids[:0]

		var fsim float64
		lastid := db.IDAcc(0)
		divb := int64(0)
		// перебираем хвост по убыванию ID, там могут быть дубли, поэтому не обрезаем заранее
		lastcnd := db.IDAcc(0)
		lastcndts := int64(0)
		lasta := int64(0)
		for icnd := len(cndlikes) - 1; icnd >= 0; icnd-- {
			v := cndlikes[icnd]

			bothid := db.SmAcc.GetLikeLiked(v)
			if myidsbmp.Contains(uint32(bothid)) {

				var a, diva int64

				if lastid != bothid {
					lastid = bothid
					lastcndts = int64(db.SmAcc.GetLikeStamp(v))
					divb = 1

					n := uint32(len(mylikes))
					i, j := uint32(0), n
					for i < j {
						h := (i + j) >> 1
						if mylikes[h].Liked() > lastid {
							i = h + 1
						} else {
							j = h
						}
					}

					a = int64(mylikes[i].Stamp)
					diva = 1

					i++
					for i < n && mylikes[i].Liked() == lastid {
						a = a + int64(mylikes[i].Stamp)
						diva++
						i++
					}

					a = a / diva

					lasta = a

				} else {
					lastcndts += int64(db.SmAcc.GetLikeStamp(v))
					divb++
					a = lasta
				}

				// считаем совместимость lastid

				b := lastcndts / divb

				if a > b {
					a, b = b, a
				}
				if a != b {
					fsim += 1 / float64(b-a)
				} else {
					fsim++
				}
			} else {
				if lastcnd != bothid && len(cndids) < 20 {
					// т.к. берем по убыванию, то тут просто добавляем
					cndids = append(cndids, bothid)
					lastcnd = bothid
				}
			}
		}

		if len(cndids) > 0 {

			for _, cndid := range cndids {
				PushHeap((*heapSims)(&sims), simrec{
					sim:  fsim,
					like: cndid,
				})
			}

			// n := uint32(len(sims))
			// i, j := uint32(0), n
			// for i < j {
			// 	h := (i + j) >> 1
			// 	if sims[h].sim > fsim {
			// 		i = h + 1
			// 	} else {
			// 		j = h
			// 	}
			// }

			// if i == n || sims[i].sim != fsim {
			// 	lnc := len(cndids)
			// 	nn := n
			// 	ii := 0
			// 	for nn < 20 {
			// 		sims = append(sims, simrec{
			// 			sim:  fsim,
			// 			like: cndids[ii],
			// 		})
			// 		nn++
			// 		ii++
			// 		if ii >= lnc {
			// 			break
			// 		}
			// 	}
			// 	if i < n {
			// 		off := uint32(lnc)
			// 		if off+i > 20 {
			// 			off = 20 - i
			// 		}
			// 		if off+i < 20 {
			// 			copy(sims[i+off:], sims[i:])
			// 		}
			// 		ii := i
			// 		for ii < i+off {
			// 			sims[ii] = simrec{
			// 				sim:  fsim,
			// 				like: cndids[ii-i],
			// 			}
			// 			ii++
			// 		}
			// 	}
			// }
		}
	}
	db.PutBitmap(candidates)
	PutCndsSlice(candids)
	PutCndsmSlice(cndids)

	db.PutLikesSlice(mylikes)

	lim := len(sims)
	res = GetCndsmSlice(heapLimit)[:lim]
	for i := lim - 1; i >= 0; i-- {
		res[i] = PopHeap((*heapSims)(&sims)).like
	}
	if lim > limit {
		res = res[:limit]
	}

	// for _, simr := range sims {
	// 	if limit <= 0 {
	// 		break
	// 	}
	// 	res = append(res, simr.like)
	// 	limit--
	// }
	PutSimSlice(sims)
	db.PutBitmap(myidsbmp)

	return
}
