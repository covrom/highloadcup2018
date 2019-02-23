package intsearch

import "unsafe"

func unsafeBinSearch(a []uint32, x uint32) uint32 {
	p := (*[1 << 28]uint32)(unsafe.Pointer(&a[0]))
	n := uint32(len(a))
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
