package flysort

import (
	"math/rand"
	"sort"
	"testing"
)

func BenchmarkParallelHeapSort100(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	a := Ints[:100]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParallelHeapSort(a)
	}
}

func BenchmarkParallelHeapSort1000(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	a := Ints[:1000]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParallelHeapSort(a)
	}
}

func BenchmarkParallelHeapSort100000(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	a := Ints[:100000]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParallelHeapSort(a)
	}
}

func BenchmarkNormalHeapSort100(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	a := Ints[:100]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NormalHeapSort(a)
	}
}

func BenchmarkNormalHeapSort1000(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	a := Ints[:1000]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NormalHeapSort(a)
	}
}

func BenchmarkNormalHeapSort100000(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	a := Ints[:100000]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NormalHeapSort(a)
	}
}

func BenchmarkStdSort100(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	a := make([]int, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(a, Ints)
		sort.Ints(a)
	}
}

func BenchmarkStdSort1000(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	a := make([]int, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(a, Ints)
		sort.Ints(a)
	}
}

func BenchmarkStdSort100000(b *testing.B) {
	if Ints == nil {
		fillInts()
	}
	a := make([]int, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(a, Ints)
		sort.Ints(a)
	}
}

func TestParallelHeapSort(t *testing.T) {
	rand.Seed(0)
	if Ints == nil {
		fillInts()
	}
	h1 := make([]int, 100)
	h2 := make([]int, 100)
	h3 := make([]int, 100)
	copy(h1, Ints)
	copy(h2, Ints)
	copy(h3, Ints)
	sort.Ints(h1)
	h2 = ParallelHeapSort(h2)
	h3 = NormalHeapSort(h3)
	for i, v := range h1 {
		if v != h2[i] || v != h3[i] {
			t.Errorf("idx=%v, v1=%v, v2=%v, v3=%v", i, v, h2[i], h3[i])
		}
	}
}
