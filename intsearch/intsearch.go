package intsearch

import (
	"sort"
)

// SearchInts searches the array for the key, returning the index of the first occurrence of the element.
func StdSearchInts(array []uint32, key uint32) uint32 {
	return uint32(sort.Search(len(array), func(i int) bool { return array[i] >= key }))
}

func InterpolationSearchInts(array []uint32, key uint32) uint32 {
	return uint32(interpSearch(array, key))
}

func BinSearchInts(array []uint32, key uint32) uint32 {
	return binSearch(array, key)
}

func BinApproxSearchInts(array []uint32, key uint32) uint32 {
	return binApproxSearch(array, key)
}

func AsmSearchInts(array []uint32, key uint32) uint32 {
	return stdSearch(array, key)
}

func LineSearchInts(array []uint32, key uint32) uint32 {
	return lineSearch(array, key)
}

// modified from http://data.linkedin.com/blog/2010/06/beating-binary-search

// Search finds the lowest value of i such that keyAt(i) = key or keyAt(i+1) > key.
func interpSearch(array []uint32, key uint32) int {
	n := len(array)
	min := array[0]
	max := array[len(array)-1]
	low, high := 0, n-1

	for {
		if key < min {
			return low
		}

		if key > max {
			return high + 1
		}

		// make a guess of the location
		var guess int
		if high == low {
			guess = high
		} else {
			size := high - low
			offset := int(float64(size-1) * (float64(key-min) / float64(max-min)))
			// offset := int((((uint64(key-min) << 32) / uint64(max-min)) * uint64(size-1)) >> 32)
			guess = low + offset
		}

		// maybe we found it?
		element := array[guess]
		if element == key {
			// scan backwards for start of value range
			for guess > 0 && array[guess-1] == key {
				guess--
			}
			return guess
		}

		// if we guessed to high, guess lower or vice versa
		if element > key {
			high = guess - 1
			max = array[high]
		} else {
			low = guess + 1
			min = array[low]
		}
	}
}

func binApproxSearch(a []uint32, x uint32) uint32 {
	n := uint32(len(a))
	if n == 0 {
		return 0
	}
	min, max := a[0], a[n-1]
	if x < min {
		return 0
	} else if x > max {
		return n
	}
	i, j := uint32(0), n
	if n > 500 {
		// интерполяционная проба границы, здесь x уже точно внутри границ
		offset := uint32(float64(n-1) * (float64(x-min) / float64(max-min)))
		probe := a[offset]
		if probe == x {
			return offset
		} else if probe < x {
			i = offset
		} else {
			j = offset
		}
	}
	for i < j {
		h := (i + j) >> 1
		if a[h] < x {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func binSearch(a []uint32, x uint32) uint32 {
	n := uint32(len(a))
	i, j := uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if a[h] < x {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func lineSearch(a []uint32, x uint32) uint32 {
	for i, v := range a {
		if x == v {
			return uint32(i)
		}
	}
	return uint32(len(a))
}
