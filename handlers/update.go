package handlers

import (
	"github.com/covrom/highloadcup2018/account"
	"github.com/covrom/highloadcup2018/db"
	"github.com/covrom/highloadcup2018/dict"
	"io"
	"strings"

	"github.com/valyala/fasthttp"
)

func UpdateAcc(ctx *fasthttp.RequestCtx, id db.IDAcc) {

	retStatus := 202
	db.SmAcc.RLock()
	i := db.SmAcc.GetIdx(id)
	db.SmAcc.RUnlock()
	if i < 0 {
		retStatus = 404
	} else {
		js := ctx.PostBody()
		acc := account.Account{}
		err := (&acc).UnmarshalJSON(js)
		if err != nil && err != account.ErrEmptyID {
			retStatus = 400
		} else {
			acc.ID = uint32(id)

			//валидация
			if len(acc.Email) > 0 {
				if !strings.ContainsRune(acc.Email, '@') {
					retStatus = 400
				}
			}

			if len(acc.Status) > 0 {
				if !(acc.Status == "свободны" || acc.Status == "заняты" || acc.Status == "всё сложно") {
					retStatus = 400
				}
			}

			if len(acc.Sex) > 0 {
				if !(acc.Sex == "f" || acc.Sex == "m") {
					retStatus = 400
				}
			}

			if retStatus == 202 {
				// есть такой же email для другого id?
				emlidx, ok := dict.DictonaryEml.In(acc.Email)
				if ok && emlidx != uint32(id) {
					retStatus = 400
				} else {
					db.SmAcc.Lock()
					smacc := db.SmAcc.Get(i)
					likes := db.GetLikesSlice(len(acc.Likes))
					smacc, likes = account.ConvertAccountUpdateSmall(acc, smacc, likes, db.SmAcc)
					db.SmAcc.Append(smacc, likes)
					db.PutLikesSlice(likes)
					db.SmAcc.Unlock()
				}
			}
		}
	}

	ctx.SetStatusCode(retStatus)
	if retStatus == 202 {
		ctx.SetContentType("application/json")
		io.WriteString(ctx, `{}`)
	}
}
