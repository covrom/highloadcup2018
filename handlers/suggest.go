package handlers

import (
	"bytes"
	"github.com/covrom/highloadcup2018/account"
	"github.com/covrom/highloadcup2018/db"
	"github.com/covrom/highloadcup2018/suggest"
	"io"
	"strconv"

	"github.com/valyala/fasthttp"
)

// Этот тип запросов похож на предыдущий тем, что он тоже про поиск "вторых половинок".
// Аналогично пересылается id пользователя, для которого мы ищем вторую половинку и аналогично используется GET-параметер limit.
// Различия в реализации. Теперь мы ищем, кого лайкают пользователи того же пола с похожими "симпатиями" и предлагаем тех,
// кого они недавно лайкали сами. В случае, если в запросе передан GET-параметр country или city, то искать "похожие симпатии"
// нужно только в определённой локации.

// Похожесть симпатий определим как функцию: similarity = f (me, account), которая вычисляется однозначно как
// сумма из дробей 1 / abs(my_like['ts'] - like['ts']),
// где my_like и like - это симпатии к одному и тому же пользователю.
// Для дроби, где my_like['ts'] == like['ts'], заменяем дробь на 1.
// Если общих лайков нет, то стоит считать пользователей абсолютно непохожими с similarity = 0.
// Если у одного аккаунта есть несколько лайков на одного и того же пользователя с разными датами, то в формуле используется среднее арифметическое их дат.

// В ответе возвращается список тех, кого ещё не лайкал пользователь с указанным id, но кого лайкали пользователи с самыми похожими симпатиями.
// Сортировка по убыванию похожести, а между лайками одного такого пользователя - по убыванию id лайка.

// Особенность 9. Если в хранимых данных не существует пользователя с переданным id, то ожидается код 404 с пустым телом ответа.

// !!!!!!!!!!!!!!!!!!
// suggest отлично ложится на индексы. 2 индекса одного типа
// Нужно быстро искать юзеров с similarity!=0. (из этого вроде очевидно какие индексы нужны)
// юзеров с similarity!=0 порядка 1000

type eventSuggest struct {
	limit int // -1 без лимита
	id    db.IDAcc
	country,
	city db.DataEntry
}

func Suggest(ctx *fasthttp.RequestCtx, id db.IDAcc) {
	retStatus := 200

	limit := 20

	ev := eventSuggest{
		id: id,
	}

	idx := db.SmAcc.GetIdx(id)
	if idx < 0 {
		retStatus = 404
	} else {
		// обходим в порядке указания параметров
		ctx.QueryArgs().VisitAll(func(k, v []byte) {
			if retStatus == 400 {
				// если была ошибка - возвращаемся
				return
			}
			if bytes.Equal(k, b_country) {
				if vidx, ok := db.SmAcc.Country.InDictonary(string(v)); ok && !db.SmAcc.Country.IsZero(vidx) {
					ev.country = vidx
				} else {
					retStatus = 400
				}
			} else if bytes.Equal(k, b_city) {
				if vidx, ok := db.SmAcc.City.InDictonary(string(v)); ok && !db.SmAcc.City.IsZero(vidx) {
					ev.city = vidx
				} else {
					retStatus = 400
				}
			} else if bytes.Equal(k, b_query_id) {
				// ничего
			} else if bytes.Equal(k, b_limit) {
				var err error
				limit, err = strconv.Atoi(string(v))
				if err != nil || limit <= 0 {
					retStatus = 400
				}
			} else {
				retStatus = 400
			}
			// здесь нельзя ничего делать, только возвращаемся
		})
		ev.limit = limit
	}

	if retStatus == 200 {
		ctx.SetContentType("application/json")
		io.WriteString(ctx, `{"accounts":[`)
		// проходим в нужном порядке через пул воркеров
		res := suggest.OneScan(ev.id, ev.country, ev.city, ev.limit)
		for j, i := range res {
			if j > 0 {
				io.WriteString(ctx, ",")
			}
			db.SmAcc.RLock()
			acc, ok := db.SmAcc.GetById(i)
			if ok {
				account.WriteSmallAccountJSON(ctx, db.SmAcc, acc, account.MaskSuggest)
			}
			db.SmAcc.RUnlock()
		}
		suggest.PutCndsmSlice(res)
		io.WriteString(ctx, `]}`)
		ctx.SetStatusCode(200)

	} else {
		ctx.Error("", retStatus)
	}
}
