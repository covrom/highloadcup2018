package suggest

const heapLimit = 21

// limited min-heap
type heapSims []simrec

func (h heapSims) Len() int { return len(h) }
func (h heapSims) Less(i, j int) bool {
	return (h[i].sim < h[j].sim) || ((h[i].sim == h[j].sim) && (h[i].like < h[j].like))
}
func (h heapSims) Swap(i, j int)  { h[i], h[j] = h[j], h[i] }
func (h *heapSims) Push(x simrec) { *h = append(*h, x) }
func (h *heapSims) Pop() simrec {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func InitHeap(h *heapSims) {
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		downHeap(h, i, n)
	}
}

func PushHeap(h *heapSims, x simrec) {
	n := h.Len()
	if n < heapLimit {
		h.Push(x)
		upHeap(h, n)
	} else {
		(*h)[0] = x
		downHeap(h, 0, n)
	}
}

func PopHeap(h *heapSims) simrec {
	n := h.Len() - 1
	h.Swap(0, n)
	downHeap(h, 0, n)
	return h.Pop()
}

func RemoveHeap(h *heapSims, i int) simrec {
	n := h.Len() - 1
	if n != i {
		h.Swap(i, n)
		if !downHeap(h, i, n) {
			upHeap(h, i)
		}
	}
	return h.Pop()
}

func FixHeap(h *heapSims, i int) {
	if !downHeap(h, i, h.Len()) {
		upHeap(h, i)
	}
}

func upHeap(h *heapSims, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func downHeap(h *heapSims, i0, n int) bool {
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
		h.Swap(i, j)
		i = j
	}
	return i > i0
}
