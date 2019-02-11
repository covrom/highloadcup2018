package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"github.com/covrom/highloadcup2018/account"
	"log"
	"os"
	"sort"
	"strings"
)

func main() {

	if len(os.Args) < 2 {
		log.Fatal("Укажите путь к data.zip")
	}
	dataFName := os.Args[1]
	r, err := zip.OpenReader(dataFName)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	data_accs := []account.Account{}

	maxid := uint32(0)
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

			data_accs = append(data_accs, accs.Accounts...)
			for _, acc := range accs.Accounts {
				if acc.ID > maxid {
					maxid = acc.ID
				}
			}

			rc.Close()
		}
	}

	minLen := 1000
	maxLen := 0
	runesEmail := make(map[string]int)
	for _, acc := range data_accs {
		if len(acc.Email) > maxLen {
			maxLen = len(acc.Email)
		}
		if len(acc.Email) < minLen {
			minLen = len(acc.Email)
		}
		s := ""
		for i, r := range acc.Email {
			if i < 3 {
				s += string(r)
			} else if i == 3 {
				s += string(r)
				cnt := runesEmail[s]
				cnt++
				runesEmail[s] = cnt
			} else {
				break
			}
		}
	}
	type remlstr struct {
		r   string
		cnt int
	}
	rems := make([]remlstr, 0, len(runesEmail))
	for k, v := range runesEmail {
		rems = append(rems, remlstr{k, v})

	}
	sort.Slice(rems, func(i, j int) bool { return rems[i].cnt > rems[j].cnt })

	fmt.Println("minLen:", minLen, "maxLen:", maxLen)
	for _, r := range rems {
		fmt.Printf("%q cnt=%d\n", r.r, r.cnt)
	}
	// go build . && ./gentest ../test_accounts/data/data.zip  >> ./stat.txt
}
