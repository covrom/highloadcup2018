package handlers

import (
	"github.com/covrom/highloadcup2018/account"
	"github.com/covrom/highloadcup2018/db"
	"io"

	"github.com/valyala/fasthttp"
)

func NewAcc(ctx *fasthttp.RequestCtx) {
	retStatus := 201
	js := ctx.PostBody()
	acc := account.Account{}
	err := (&acc).UnmarshalJSON(js)
	if err != nil {
		retStatus = 400
	} else {
		db.SmAcc.RLock()
		ok := db.SmAcc.Contains(db.IDAcc(acc.ID))
		db.SmAcc.RUnlock()
		if ok {
			retStatus = 400
		} else {
			db.SmAcc.Lock()
			smacc, likes := account.ConvertAccountToSmall(acc, db.SmAcc, false)
			db.SmAcc.Append(smacc, likes)
			db.SmAcc.Unlock()
		}
	}

	ctx.SetStatusCode(retStatus)
	if retStatus == 201 {
		ctx.SetContentType("application/json")
		io.WriteString(ctx, `{}`)
	}
}
