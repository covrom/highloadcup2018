package flysort

import (
	"math/rand"
	"testing"
)

var Ints heapInts

func fillInts() {
	rand.Seed(0)

	Ints = make(heapInts, 1000000)

	for i := 0; i < heapLimit; i++ {
		Ints[i] = int(rand.Int() % heapLimit)
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
		h = SortInsert(h, Ints[elt])
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
		h = LineInsert(h, Ints[elt])
		elt++
		if elt >= len(Ints) {
			elt = 0
		}
	}
}

func BenchLineUnrollInsert(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	elt := 0
	h := make(heapInts, 0, heapLimit)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h = LineUnrollInsert(h, Ints[elt])
		elt++
		if elt >= len(Ints) {
			elt = 0
		}
	}
}

func BenchBubbleInsert(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	elt := 0
	h := make(heapInts, 0, heapLimit)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h = BubbleInsert(h, Ints[elt])
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
	b.Run("SortInsert100", BenchSortInsert)
	b.Run("LineInsert100", BenchLineInsert)
	b.Run("LineUnrollInsert100", BenchLineUnrollInsert)
	b.Run("BubbleInsert100", BenchBubbleInsert)

	heapLimit = 1000
	b.Run("SortInsert1000", BenchSortInsert)
	b.Run("LineInsert1000", BenchLineInsert)
	b.Run("LineUnrollInsert1000", BenchLineUnrollInsert)
	b.Run("BubbleInsert1000", BenchBubbleInsert)

	heapLimit = 10000
	b.Run("SortInsert10000", BenchSortInsert)
	b.Run("LineInsert10000", BenchLineInsert)
	b.Run("LineUnrollInsert10000", BenchLineUnrollInsert)
	b.Run("BubbleInsert10000", BenchBubbleInsert)

	heapLimit = 100000
	b.Run("SortInsert100000", BenchSortInsert)
	b.Run("LineInsert100000", BenchLineInsert)
	b.Run("LineUnrollInsert100000", BenchLineUnrollInsert)
	b.Run("BubbleInsert100000", BenchBubbleInsert)
}

func TestAll(t *testing.T) {

	rand.Seed(0)

	if Ints == nil {
		fillInts()
	}

	heapLimit = 50

	h1 := make(heapInts, 0, heapLimit)
	h2 := make(heapInts, 0, heapLimit)
	h3 := make(heapInts, 0, heapLimit)
	h4 := make(heapInts, 0, heapLimit)

	elt := 0
	for i := 0; i < 100; i++ {
		v := Ints[elt]
		h1 = SortInsert(h1, v)
		h2 = LineInsert(h2, v)
		h3 = LineUnrollInsert(h3, v)
		h4 = BubbleInsert(h4, v)
		elt++
		if elt >= len(Ints) {
			elt = 0
		}
	}

	t.Log(len(h1))
	t.Log(h1)
	t.Log(len(h2))
	t.Log(h2)
	t.Log(len(h3))
	t.Log(h3)
	t.Log(len(h4))
	t.Log(h4)

	for i := range h2 {
		v1 := h1[i]
		v2 := h2[i]
		v3 := h3[i]
		v4 := h4[i]
		if v1 != v2 || v2 != v3 || v3 != v4 {
			t.Errorf("idx=%v, v1=%v, v2=%v, v3=%v, v4=%v", i, v1, v2, v3, v4)
		}
	}
}
