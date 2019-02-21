package flysort

import (
	"math/rand"
	"testing"
)

var Ints heapInts

func fillInts() {
	rand.Seed(0)

	Ints = make(heapInts, heapLimit*10000)

	for i := 0; i < heapLimit; i++ {
		Ints[i] = int(rand.Int() % heapLimit)
	}
}

func BenchHeapInsert(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	elt := 0
	h := make(heapInts, 0, heapLimit)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PushHeap(&h, Ints[elt])
		elt++
		if elt >= len(Ints) {
			elt = 0
		}
	}
}

func BenchSortInsert(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	elt := 0
	h := make(heapInts, 0, heapLimit)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SortInsert(&h, Ints[elt])
		elt++
		if elt >= len(Ints) {
			elt = 0
		}
	}
}

func BenchLineInsert(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	elt := 0
	h := make(heapInts, 0, heapLimit)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LineInsert(&h, Ints[elt])
		elt++
		if elt >= len(Ints) {
			elt = 0
		}
	}
}

func BenchmarkInserts(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	heapLimit = 100
	b.Run("HeapInsert100", BenchHeapInsert)
	b.Run("SortInsert100", BenchSortInsert)
	b.Run("LineInsert100", BenchLineInsert)

	heapLimit = 1000
	b.Run("HeapInsert1000", BenchHeapInsert)
	b.Run("SortInsert1000", BenchSortInsert)
	b.Run("LineInsert1000", BenchLineInsert)

	heapLimit = 10000
	b.Run("HeapInsert10000", BenchHeapInsert)
	b.Run("SortInsert10000", BenchSortInsert)
	b.Run("LineInsert10000", BenchLineInsert)
}

func TestAll(t *testing.T) {

	rand.Seed(0)

	if Ints == nil {
		fillInts()
	}

	heapLimit = 100

	h1 := new(heapInts)
	h2 := new(heapInts)
	h3 := new(heapInts)

	elt := 0
	for i := 0; i < 100000; i++ {
		PushHeap(h1, Ints[elt])
		SortInsert(h2, Ints[elt])
		LineInsert(h3, Ints[elt])
		elt++
		if elt >= len(Ints) {
			elt = 0
		}
	}

	for i := range *h3 {

		v1 := PopHeap(h1)
		v2 := (*h2)[i]
		v3 := (*h3)[i]

		if v1 != v2 || v2 != v3 {
			t.Errorf("idx=%v, v1=%v, v2=%v, v3=%v", i, v1, v2, v3)
		}

	}
}
