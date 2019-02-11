package main

import (
	"fmt"
	"github.com/covrom/highloadcup2018/intsearch"
	"math/rand"
)

// go build -gcflags -S ./main.go

func main() {
	rand.Seed(0)

	const limit = 100

	ints := make([]uint32, limit)

	for i := range ints {
		ints[i] = uint32(i)
	}

	for want, q := range ints {
		if idx := intsearch.AsmSearchInts(ints, q); idx != uint32(want) {
			fmt.Printf("StdSearchInts(ints, %v)=%v, want %v\n", q, idx, want)
		}
		if idx := intsearch.StdSearchInts(ints, q); idx != uint32(want) {
			fmt.Printf("SearchInts(ints, %v)=%v, want %v\n", q, idx, want)
		}
	}
}
