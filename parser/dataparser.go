package parser

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/covrom/highloadcup2018/account"
	"github.com/covrom/highloadcup2018/db"
	"github.com/covrom/highloadcup2018/handlers"
	"io/ioutil"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	dataFName = "/tmp/data/data.zip"
)

func ParseData() {

	r, err := zip.OpenReader(dataFName)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	sz := uint64(0)
	for _, f := range r.File {
		sz += f.UncompressedSize64 / 1024 / 1024
	}
	log.Println("Zip contains:", len(r.File), " size ", sz, "M")

	fulllen := 0

	fndtime := false

	b, err := ioutil.ReadFile("/tmp/data/options.txt")
	if err == nil {
		bs := bytes.Split(b, []byte("\n"))
		db.CurrentTime = db.TimeStamp(bs[0]).Int()
		fndtime = true
	}

	if !fndtime {
		for _, f := range r.File {
			if f.Name == "options.txt" {
				rc, err := f.Open()
				if err != nil {
					break
				}
				b, err := ioutil.ReadAll(rc)
				if err != nil {
					break
				}
				bs := bytes.Split(b, []byte("\n"))
				db.CurrentTime = db.TimeStamp(bs[0]).Int()
				rc.Close()
				fndtime = true
			}
		}
	}

	if !fndtime {
		log.Println("Timestamp not found")
	}

	chset := make(chan account.Account, 10000)
	wgset := &sync.WaitGroup{}
	wgset.Add(1)
	go func() {
		defer wgset.Done()
		for acc := range chset {
			smacc, likes := account.ConvertAccountToSmall(acc, db.SmAcc, true)
			db.SmAcc.Append(smacc, likes)
		}
	}()

	for _, f := range r.File {
		if strings.HasPrefix(f.Name, "accounts_") && strings.HasSuffix(f.Name, ".json") {
			rc, err := f.Open()
			if err != nil {
				log.Println(err)
				continue
			}

			accs := &account.Accounts{}
			dec := json.NewDecoder(rc)

			err = dec.Decode(accs)
			if err != nil {
				log.Println(err)
				continue
			}

			for _, acc := range accs.Accounts {
				chset <- acc
			}

			fulllen += len(accs.Accounts)

			accs = nil
			dec = nil
			rc.Close()
			rc = nil

			runtime.GC()

			memstat := &runtime.MemStats{}
			runtime.ReadMemStats(memstat)
			if memstat.HeapAlloc/1024/1024 > 1750 {
				fmt.Println(fulllen, memstat.HeapAlloc/1024/1024, ";")
			}
		}
	}

	close(chset)
	wgset.Wait()
	runtime.GC()

	memstat := &runtime.MemStats{}
	runtime.ReadMemStats(memstat)

	cgswlim, _ := ioutil.ReadFile("/sys/fs/cgroup/memory/memory.memsw.limit_in_bytes")

	log.Println(fulllen, memstat.HeapAlloc/1024, " swlim ", string(cgswlim))

	time.AfterFunc(60*time.Second, memstats)

	// intereststat()
	// filterstat()
	// groupstat()
}

func filterstat() {
	handlers.FilterStats = handlers.NewFilterStat()
	time.AfterFunc(200*time.Second, func() {
		log.Println(handlers.FilterStats)
	})
}

func groupstat() {
	handlers.FilterStats = handlers.NewFilterStat()
	handlers.FilterStats.GroupKeys = true
	time.AfterFunc(120*time.Second, func() {
		log.Println(handlers.FilterStats)
	})
}

func memstats() {
	memstat := &runtime.MemStats{}
	runtime.ReadMemStats(memstat)
	log.Println("heap:", memstat.HeapAlloc/1024)
	time.AfterFunc(10*time.Second, memstats)
	runtime.GC()
}

func intereststat() {
	m := make(map[string]int)
	iter := db.SmAcc.Iterator()
	for iter.HasNext() {
		acc := iter.Next()
		ints := db.SmAcc.Interests.GetStringSet(acc.ID)
		for _, v := range ints {
			m[v] += 1
		}
	}
	keys := make([]string, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] > keys[j] })
	for _, k := range keys {
		fmt.Println(k, m[k])
	}
}
