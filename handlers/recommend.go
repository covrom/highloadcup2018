package handlers

import (
	"bytes"
	"github.com/covrom/highloadcup2018/account"
	"github.com/covrom/highloadcup2018/db"
	"github.com/covrom/highloadcup2018/recommend"
	"io"
	"strconv"

	"github.com/valyala/fasthttp"
)

var (
	b_country  = []byte("country")
	b_city     = []byte("city")
	b_query_id = []byte("query_id")
	b_limit    = []byte("limit")
)

type eventRecommend struct {
	limit int // -1 без лимита
	id    db.IDAcc
	country,
	city db.DataEntry
}

func Recommend(ctx *fasthttp.RequestCtx, id db.IDAcc) {

	retStatus := 200

	limit := 20

	ev := eventRecommend{
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
		res := recommend.OneScan(ev.id, ev.country, ev.city, ev.limit)
		for j, i := range res {
			if j > 0 {
				io.WriteString(ctx, ",")
			}
			db.SmAcc.RLock()
			acc, ok := db.SmAcc.GetById(i)
			if ok {
				account.WriteSmallAccountJSON(ctx, db.SmAcc, acc, account.MaskRecommend)
			}
			db.SmAcc.RUnlock()
		}
		recommend.PutCndsmSlice(res)
		io.WriteString(ctx, `]}`)
		ctx.SetStatusCode(200)

	} else {
		ctx.Error("", retStatus)
	}
}
