package recommend

import (
	"github.com/covrom/highloadcup2018/db"
	"sync"
)

// Рекомендации по совместимости: /accounts/<id>/recommend/

// Данный запрос используется для поиска "второй половинки" по указанным пользовательским данным.
// В запросе передаётся id пользователя, для которого ищутся те, кто лучше всего совместимы по статусу, возрасту и интересам.
// Решение должно проверять совместимость только с противоположным полом (мы не против секс-меньшинств и осуждаем дискриминацию, просто так получилось :) ).
// Если в GET-запросе передана страна или город с ключами country и city соответственно, то нужно искать только среди живущих в указанном месте.

// В ответе ожидается код 200 и структура {"accounts": [ ... ]} либо код 404 , если пользователя с искомым id не обнаружено в хранимых данных.
// По ключу "accounts" должны быть N пользователей, сортированных по убыванию их совместимости с обозначенным id.
// Число N задаётся в запросе GET-параметром limit и не бывает больше 20.

// Совместимость определяется как функция от двух пользователей: compatibility = f (me, somebody).
// Функция строится самими участниками, но так, чтобы соответствовать следующим правилам:

// Наибольший вклад в совместимость даёт наличие статуса "свободны". Те кто "всё сложно" идут во вторую очередь,
// а "занятые" в третью и последнюю (очень вероятно их вообще не будет в ответе).
// Далее идёт совместимость по интересам. Чем больше совпавших интересов у пользователей, тем более они совместимы.
// Третий по значению параметр - различие в возрасте. Чем больше разница, тем меньше совместимость.
// Те, у кого активирован премиум-аккаунт, пропихиваются в самый верх, вперёд обычных пользователей.
// Если таких несколько, то они сортируются по совместимости между собой.
// Если общих интересов нет, то стоит считать пользователей абсолютно несовместимыми с compatibility = 0.
// В итоговом списке необходимо выводить только следующие поля: id, email, status, fname, sname, birth, premium, interests.
// Если в ответе оказались одинаково совместимые пользователи (одни и те же status, interests, birth), то выводить их по возрастанию id

// 20.12.2018: в этом запросе теперь не нужно выводить данные по interests. Сделано по той же причине, что и в запросах /filter/.

// Пример запроса и корректного ответа на него:

// GET: /accounts/89528/recommend/?country=Индция&limit=8&query_id=151
// (вернуть 8 самых совместимых с пользователем id=89528 в стране "Индция")

// {
//     "accounts":  [
//         {
//             "email": "heernetletem@me.com",
//             "premium": {"finish": 1546029018.0, "start": 1530304218},
//             "status": "свободны",
//             "sname": "Данашевен",
//             "fname": "Анатолий",
//             "id": 35473,
//             "birth": 926357446
//         },{
//             "email": "teicfiwidadsuna@inbox.com",
//             "premium": {"finish": 1565741391.0, "start": 1534205391},
//             "status": "свободны",
//             "id": 23067,
//             "birth": 801100962
//         },{
//             "email": "nonihiwwahigtegodyn@inbox.com",
//             "premium": {"finish": 1557069862.0, "start": 1525533862},
//             "status": "свободны",
//             "sname": "Стаметаный",
//             "fname": "Виталий",
//             "id": 90883,
//             "birth": 773847481
//         }
//     ]
// }
// Особенность 8. Если в хранимых данных не существует пользователя с переданным id, то ожидается код 404 с пустым телом ответа.

// WHERE commonInterests>0 ORDER BY premium_now, status, commonInterests, ageDiffSeconds
// сначала идут "занятые" старички с премиумом, а потом "свободные" подходящего возраста

var (
	StatusFree, StatusTricky, StatusBusy db.DataEntry
)

type comptRec struct {
	id db.IDAcc
	v  uint64
}

func compatible(cmpid, self db.IDAcc, selfintrs []db.DataEntry, selfaccBirth int32, variant db.DataEntry) uint64 {
	acc, _ := db.SmAcc.GetById(cmpid)

	highScore := uint64(variant * 100)

	ageDiff := acc.Birth - selfaccBirth
	if ageDiff < 0 {
		ageDiff = -ageDiff
	}
	ageDiff = 2147483647 - ageDiff

	cmpintrs := db.SmAcc.Interests.GetSet(cmpid)

	cntintrs := 0
	for _, vcmp := range cmpintrs {
		fnd := false
		for _, vself := range selfintrs {
			if vcmp == vself {
				fnd = true
				break
			}
		}
		if fnd {
			cntintrs++
		}
	}
	res := ((highScore + uint64(cntintrs)) << 32) | uint64(ageDiff)
	return res
}

var cndsmPool = sync.Pool{}

func GetCndsmSlice(c int) []db.IDAcc {
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

	selfacc, _ := db.SmAcc.GetById(id)

	sex := 1 - db.SmAcc.Sex.Get(id)

	var iter, fltiter db.IDIterator

	if city > 0 {
		fltiter = db.SmAcc.SexCity.IteratorWithFilterVal(city<<1|sex, false, false)
	} else if country > 0 {
		fltiter = db.SmAcc.SexCountry.IteratorWithFilterVal(country<<1|sex, false, false)
	}

	variants := []db.DataEntry{
		(sex << 3) | 1<<2 | StatusFree,   // premium=1
		(sex << 3) | 1<<2 | StatusTricky, // premium=1
		(sex << 3) | 1<<2 | StatusBusy,   // premium=1
		(sex << 3) | StatusFree,          // premium=0
		(sex << 3) | StatusTricky,        // premium=0
		(sex << 3) | StatusBusy,          // premium=0
	}

	compt := make([]comptRec, 0, 20)

	// только те, у которых есть совпадающие интересы
	selfintrs := db.SmAcc.Interests.GetSet(id)
	if len(selfintrs) == 0 {
		return
	}
	merge_iters := make([]db.IDIterator, len(selfintrs))

	for _, variant := range variants {

		for i, dv := range selfintrs {
			merge_iters[i] = db.SmAcc.SexPremiumStatusInterest.IteratorWithFilterVal(variant*100+dv, false, false)
		}
		mrgiter := db.NewMergeIterator(merge_iters...)

		if fltiter != nil {
			iterintersect := db.NewIteratorIntersect(false)
			iterintersect.Append(mrgiter)
			iterintersect.Append(fltiter.Clone())
			iter = iterintersect
		} else {
			iter = mrgiter
		}

		for iter.HasNext() {
			cmpid := iter.NextID()

			v := compatible(cmpid, id, selfintrs, selfacc.Birth, variant)

			n := uint32(len(compt))
			i, j := uint32(0), n
			for i < j {
				h := (i + j) >> 1
				if !((compt[h].v < v) || ((compt[h].v == v) && (compt[h].id >= cmpid))) {
					i = h + 1
				} else {
					j = h
				}
			}

			if n < uint32(limit) {
				compt = append(compt, comptRec{
					id: cmpid,
					v:  v,
				})
			}
			if i < n {
				copy(compt[i+1:], compt[i:])
				compt[i] = comptRec{
					id: cmpid,
					v:  v,
				}
			}
		}

		// если все заполнили - следующий вариант не актуален
		if len(compt) >= limit {
			break
		}
	}
	res = GetCndsmSlice(len(compt))
	for _, v := range compt {
		res = append(res, v.id)
	}

	return
}
