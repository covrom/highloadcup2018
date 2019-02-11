package handlers

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

var FilterStats *FilterStat

type statRec struct {
	cnt   int
	dmsec int
}

type FilterStat struct {
	mu        sync.Mutex
	statMap   map[string]statRec
	maxLimit  int
	GroupKeys bool
	keysMap   map[string]int
}

func NewFilterStat() *FilterStat {
	fst := &FilterStat{
		statMap: make(map[string]statRec),
		keysMap: make(map[string]int),
	}
	return fst
}

func (fst *FilterStat) Add(ss []string, d time.Duration) {
	fst.mu.Lock()
	defer fst.mu.Unlock()

	sort.Strings(ss)

	name := strings.Join(ss, ",")
	cnt := fst.statMap[name]
	cnt.cnt++
	cnt.dmsec += int(d / time.Millisecond)
	fst.statMap[name] = cnt

}

type fstLine struct {
	name  string
	cnt   int
	dmsec int
}

func (fst *FilterStat) String() string {
	fst.mu.Lock()
	defer fst.mu.Unlock()

	s := &strings.Builder{}
	strs := make([]fstLine, 0, len(fst.statMap))
	for k, v := range fst.statMap {
		strs = append(strs, fstLine{name: k, cnt: v.cnt, dmsec: v.dmsec})
	}
	sort.Slice(strs, func(i, j int) bool {
		return strs[i].dmsec > strs[j].dmsec || (strs[i].dmsec == strs[j].dmsec && strs[i].cnt > strs[j].cnt)
	})
	fmt.Fprintf(s, "Max limit : %d\n", fst.maxLimit)
	if fst.GroupKeys {
		strs2 := make([]fstLine, 0, len(fst.keysMap))
		for k, v := range fst.keysMap {
			strs2 = append(strs2, fstLine{name: k, cnt: v})
		}
		sort.Slice(strs2, func(i, j int) bool {
			return strs2[i].cnt > strs2[j].cnt
		})
		for _, v := range strs2 {
			fmt.Fprintf(s, "keys[%s] : %d\n", v.name, v.cnt)
		}
	}
	for _, v := range strs {
		fmt.Fprintf(s, "[%s] : %d %dms\n", v.name, v.cnt, v.dmsec)
	}

	return s.String()
}

var b_keys = []byte("keys")

func (fst *FilterStat) AddQueryArgs(qa *fasthttp.Args, d time.Duration) {
	args := make([]string, 0)
	qa.VisitAll(func(k, v []byte) {
		if !bytes.Equal(k, b_query_id) {
			if fst.GroupKeys && bytes.Equal(k, b_keys) {
				keys := strings.Split(string(v), ",")
				sort.Strings(keys)
				ks := strings.Join(keys, ",")
				args = append(args, string(k)+"["+ks+"]")
				cnt := fst.keysMap[ks]
				cnt++
				fst.keysMap[ks] = cnt
			} else {
				args = append(args, string(k))
			}
		}
		if bytes.Equal(k, b_limit) {
			lim, err := strconv.Atoi(string(v))
			if err == nil && lim > fst.maxLimit {
				fst.maxLimit = lim
			}
		}
	})
	fst.Add(args, d)
}
