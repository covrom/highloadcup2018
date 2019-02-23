package intsearch

import "unsafe"

// see https://github.com/golang/go/issues/30366
func unsafeBinSearch(a []uint32, x uint32) uint32 {
	n := uint32(len(a))

	p := (*[1 << 31]uint32)(unsafe.Pointer(&a[0]))
	if n == 0 || p == nil {
		return 0
	}

	i, j := uint32(0), n
	for i < j {
		h := (i + j) >> 1
		if p[h] < x {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}
