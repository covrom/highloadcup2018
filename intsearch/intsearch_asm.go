// +build amd64

package intsearch

//go:noescape

func stdSearch(a []uint32, x uint32) uint32

// {
// 	n := uint32(len(a))
// 	i, j := uint32(0), n
// 	for i < j {
// 		h := (i + j) >> 1
// 		if a[h] < x {
// 			i = h + 1
// 		} else {
// 			j = h
// 		}
// 	}
// 	return i
// }
