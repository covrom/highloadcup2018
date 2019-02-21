package flysort

// limited min-heap
type heapInts []int

func (h heapInts) Less(i, j int) bool {
	return h[i] < h[j]
}

func InitHeap(h *heapInts) {
	n := len(*h)
	for i := n/2 - 1; i >= 0; i-- {
		downHeap(h, i, n)
	}
}

func PushHeap(h *heapInts, x int) {
	n := len(*h)
	if n < heapLimit {
		*h = append(*h, x)
		upHeap(h, n)
	} else {
		(*h)[0] = x
		downHeap(h, 0, n)
	}
}

func PopHeap(h *heapInts) int {
	n := len(*h) - 1
	(*h)[0], (*h)[n] = (*h)[n], (*h)[0]
	downHeap(h, 0, n)
	x := (*h)[n]
	*h = (*h)[0 : n-1]
	return x
}

func RemoveHeap(h *heapInts, i int) int {
	n := len(*h) - 1
	if n != i {
		(*h)[i], (*h)[n] = (*h)[n], (*h)[i]
		if !downHeap(h, i, n) {
			upHeap(h, i)
		}
	}
	x := (*h)[n]
	*h = (*h)[0 : n-1]
	return x
}

func FixHeap(h *heapInts, i int) {
	if !downHeap(h, i, len(*h)) {
		upHeap(h, i)
	}
}

func upHeap(h *heapInts, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
		j = i
	}
}

func downHeap(h *heapInts, i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
		i = j
	}
	return i > i0
}
