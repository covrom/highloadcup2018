package handlers

import (
	"fmt"
	"github.com/covrom/highloadcup2018/db"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"
)

func SelectGet(ctx *fasthttp.RequestCtx) {
	// done := make(chan struct{})
	// defer close(done)
	// go monitorTime(ctx, done)

	if FilterStats != nil {
		tnow := time.Now()
		defer func() { FilterStats.AddQueryArgs(ctx.QueryArgs(), time.Since(tnow)) }()
	}

	s, ok := ctx.UserValue("uid").(string)
	if !ok {
		ctx.SetStatusCode(404)
		return
	}
	switch s {
	case "filter":
		if ctx.UserValue("pth") != nil {
			ctx.SetStatusCode(404)
			return
		}
		Filter(ctx)
	case "group":
		if ctx.UserValue("pth") != nil {
			ctx.SetStatusCode(404)
			return
		}
		Group(ctx)
	default:
		i, err := strconv.Atoi(s)
		if err != nil {
			ctx.SetStatusCode(404)
			return
		}
		switch ctx.UserValue("pth") {
		case "suggest":
			Suggest(ctx, db.IDAcc(i))
		case "recommend":
			Recommend(ctx, db.IDAcc(i))
		default:
			ctx.SetStatusCode(404)
			return
		}
	}
}

func SelectPost(ctx *fasthttp.RequestCtx) {
	// done := make(chan struct{})
	// defer close(done)
	// go monitorTime(ctx, done)

	if FilterStats != nil {
		tnow := time.Now()
		defer func() { FilterStats.AddQueryArgs(ctx.QueryArgs(), time.Since(tnow)) }()
	}

	s, ok := ctx.UserValue("uid").(string)
	if !ok {
		ctx.SetStatusCode(404)
		return
	}
	if ctx.UserValue("wrong") != nil {
		ctx.SetStatusCode(404)
		return
	}
	switch s {
	case "new":
		NewAcc(ctx)
	case "likes":
		Likes(ctx)
	default:
		i, err := strconv.Atoi(s)
		if err != nil {
			ctx.SetStatusCode(404)
			return
		}
		UpdateAcc(ctx, db.IDAcc(i))
	}
}

func monitorTime(ctx *fasthttp.RequestCtx, done chan struct{}) {
	const d = 1000 * time.Millisecond
	tmr := time.NewTimer(d)
	defer tmr.Stop()
	for {
		select {
		case <-tmr.C:
			// if FilterStats != nil {
			// 	FilterStats.AddQueryArgs(ctx.QueryArgs(), d)
			// }
			fmt.Println("overload:", ctx.RequestURI())
			return
		case <-done:
			return
		}
	}
}
