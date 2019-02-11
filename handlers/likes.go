package handlers

import (
	"github.com/covrom/highloadcup2018/account"
	"github.com/covrom/highloadcup2018/db"
	"io"

	"github.com/valyala/fasthttp"
)

type oneLike struct {
	likee db.IDAcc
	ts    int32
}

func Likes(ctx *fasthttp.RequestCtx) {
	retStatus := 202
	js := ctx.PostBody()
	likes := account.UpdateLikes{}
	err := (&likes).UnmarshalJSON(js)
	if err != nil {
		retStatus = 400
	} else {
		accs := make(map[int32][]oneLike, len(likes.Likes))
		for _, like := range likes.Likes {
			db.SmAcc.RLock()
			ifrom := db.SmAcc.GetIdx(db.IDAcc(like.Liker))
			db.SmAcc.RUnlock()
			if ifrom >= 0 {
				db.SmAcc.RLock()
				ok := db.SmAcc.Contains(db.IDAcc(like.Likee))
				db.SmAcc.RUnlock()
				if ok {
					lks, ok := accs[ifrom]
					if !ok {
						lks = make([]oneLike, 0, 10)
					}
					lks = append(lks, oneLike{
						likee: db.IDAcc(like.Likee),
						ts:    like.Stamp.Int(),
					})
					accs[ifrom] = lks
				} else {
					retStatus = 400
					break
				}
			} else {
				retStatus = 400
				break
			}
		}
		if retStatus == 202 {
			for idx, ls := range accs {
				db.SmAcc.Lock()
				acc := db.SmAcc.Get(idx)
				acclikes := make([]db.Like, len(ls))
				for i, l := range ls {
					acclikes[i] = db.NewLike(
						acc.ID,
						l.likee,
						l.ts,
					)
				}
				db.SmAcc.Append(acc, acclikes)
				db.SmAcc.Unlock()
			}
		}
	}

	ctx.SetStatusCode(retStatus)
	if retStatus == 202 {
		ctx.SetContentType("application/json")
		io.WriteString(ctx, `{}`)
	}
}
