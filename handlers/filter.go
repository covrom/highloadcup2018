package handlers

import (
	"github.com/covrom/highloadcup2018/account"
	"github.com/covrom/highloadcup2018/db"
	"github.com/covrom/highloadcup2018/queryset"
	"io"

	"github.com/valyala/fasthttp"
)

func Filter(ctx *fasthttp.RequestCtx) {
	// статистика
	// if FilterStats != nil {
	// 	FilterStats.AddQueryArgs(ctx.QueryArgs())
	// }

	// выстраиваем цепочку индексов
	// сначала жадно ищем покрывающий индекс, и если не нашли - добавляем одинарный обработчик
	// индекс является функционалом - он производит переработку текущего сета, переданного ему на вход
	// если индекс может отдать биткарту для And-операции,
	//   то он это делает, и затем результаты нескольких таких фильтров обединяются в одну биткарту,
	//   которая применяется к текущему сету

	retStatus := 200

	qs := queryset.NewQuerySet()

	ctx.QueryArgs().VisitAll(func(k, v []byte) {
		if retStatus == 400 {
			return
		}
		if ok := qs.SetKVFilter(k, v); !ok {
			retStatus = 400
		}
	})

	if retStatus == 200 {
		qs.Validate()
		if !qs.HasBadVals {
			queryset.OneScan(qs)
		}
		if qs.HasBadVals {
			ctx.Error("", 400)
		} else {
			ctx.SetContentType("application/json")
			io.WriteString(ctx, `{"accounts":[`)
			// проходим в нужном порядке через пул воркеров
			for j, i := range qs.SortedResult {
				if j > 0 {
					io.WriteString(ctx, ",")
				}
				db.SmAcc.RLock()
				acc, ok := db.SmAcc.GetById(i)
				if ok {
					account.WriteSmallAccountJSON(ctx, db.SmAcc, acc, qs.FieldMap) //lks
				}
				db.SmAcc.RUnlock()
			}
			io.WriteString(ctx, `]}`)
			ctx.SetStatusCode(200)
		}
	} else {
		ctx.Error("", retStatus)
	}

	qs.Close()
}
