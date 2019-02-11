package main

import (
	"flag"
	"github.com/covrom/highloadcup2018/account"
	"github.com/covrom/highloadcup2018/db"
	"github.com/covrom/highloadcup2018/handlers"
	"github.com/covrom/highloadcup2018/parser"
	"github.com/covrom/highloadcup2018/recommend"
	"log"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"syscall"
	"time"

	// "net/http"
	// _ "net/http/pprof"

	// "github.com/pkg/profile"

	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
)

var (
	// Важно - сервер должен слушать 80-й порт, чтобы обстрел прошел успешно!
	// Запросы идут с заголовком Host: accounts.com
	// по протоколу HTTP/1.1 с переиспользуемыми соединениями (keep-alive).
	// Сетевые потери полностью отсутствуют.
	addr = flag.String("addr", ":80", "addr:port")
)

func onShutdown(f func()) {
	once := &sync.Once{}
	sigc := make(chan os.Signal, 3)
	signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)
	go func() {
		<-sigc
		once.Do(f)
	}()
}

func main() {

	flag.Parse()

	zt, err := time.Parse(time.RFC3339, "1949-12-31T23:59:59Z")
	if err != nil {
		log.Fatal(err)
	}
	db.NullTime = int32(zt.Unix())

	account.RePhoneCode = regexp.MustCompile(`\([0-9]+\)`)

	recommend.StatusBusy = db.SmAcc.Status.ToDictonary("заняты")       //1
	recommend.StatusTricky = db.SmAcc.Status.ToDictonary("всё сложно") //2
	recommend.StatusFree = db.SmAcc.Status.ToDictonary("свободны")     //3

	// go tool pprof -seconds 180 ./highloadcup2018 http://127.0.0.1:8080/debug/pprof/profile
	// go tool pprof -alloc_objects ./highloadcup2018 http://127.0.0.1:8080/debug/pprof/heap
	// go tool pprof -inuse_space ./highloadcup2018 http://127.0.0.1:8080/debug/pprof/heap

	// go http.ListenAndServe(":8080", nil)

	parser.ParseData()

	// profiler := profile.Start(profile.TraceProfile, profile.ProfilePath("."))
	// defer profiler.Stop()

	router := fasthttprouter.New()
	router.GET("/accounts/:uid/", handlers.SelectGet)
	router.GET("/accounts/:uid/:pth/", handlers.SelectGet)
	router.POST("/accounts/:uid/", handlers.SelectPost)
	router.POST("/accounts/:uid/:wrong/", handlers.SelectPost)

	s := &fasthttp.Server{
		Handler: router.Handler,
		// Concurrency:          4000,
		// ReadTimeout:  3 * time.Second,
		// WriteTimeout: 3 * time.Second,
		// MaxConnsPerIP:        4000,
		// MaxRequestsPerConn: 1,
		// MaxKeepaliveDuration: 60 * time.Minute,
		ReduceMemoryUsage: true,
		// LogAllErrors:         true,
	}
	if err := s.ListenAndServe(*addr); err != nil {
		log.Fatalf("error in ListenAndServe: %s", err)
	}

	// go get -u github.com/atercattus/highloadcup_tester
	// highloadcup_tester -addr http://127.0.0.1:8000 -hlcupdocs ./test_accounts/ -test -phase 1 -utf8 -filter "filter"
	// highloadcup_tester -addr http://127.0.0.1:8000 -hlcupdocs ./test_accounts/ -tank 300 -time 180s -phase 1 -utf8 -filter "filter"

}
