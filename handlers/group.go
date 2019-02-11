package handlers

import (
	"github.com/covrom/highloadcup2018/groupset"

	"github.com/valyala/fasthttp"
)

func Group(ctx *fasthttp.RequestCtx) {
	retStatus := 200

	gs := groupset.NewGroupSet()

	ctx.QueryArgs().VisitAll(func(k, v []byte) {
		if retStatus == 400 {
			return
		}
		if ok := gs.SetKVFilter(k, v); !ok {
			retStatus = 400
		}
		if gs.HasBadVals {
			retStatus = 400
		}
	})

	if retStatus == 200 {
		gs.Validate()
		if !gs.HasBadVals {
			groupset.OneScan(gs)
		}
		if gs.HasBadVals {
			ctx.Error("", 400)
		} else {
			ctx.SetContentType("application/json")
			gs.MarshalResult(ctx)
			ctx.SetStatusCode(200)
		}
	} else {
		ctx.Error("", retStatus)
	}

	gs.Close()
}
