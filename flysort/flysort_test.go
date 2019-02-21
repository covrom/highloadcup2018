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
