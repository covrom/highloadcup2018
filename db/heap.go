package db

type ElemHeapIDAcc struct {
	ID       IDAcc      // Value of this element.
	Iterator IDIterator // Which list this element comes from.
}

type IDAccHeap struct {
	reverse bool
	Elems   []ElemHeapIDAcc
}

func NewIDAccHeap(reverse bool, capacity int) *IDAccHeap {
	return &IDAccHeap{
		reverse: reverse,
		Elems:   make([]ElemHeapIDAcc, 0, capacity),
	}
}

func (h *IDAccHeap) Clone() *IDAccHeap {
	rv := NewIDAccHeap(h.reverse, cap(h.Elems))
	for _, el := range h.Elems {
		v := ElemHeapIDAcc{
			ID:       el.ID,
			Iterator: el.Iterator.Clone(),
		}
		rv.Elems = append(rv.Elems, v)
	}
	return rv
}

func (h *IDAccHeap) Len() int { return len(h.Elems) }
func (h *IDAccHeap) Less(i, j int) bool {
	if h.reverse {
		return h.Elems[i].ID > h.Elems[j].ID
	} else {
		return h.Elems[i].ID < h.Elems[j].ID
	}
}
func (h *IDAccHeap) Swap(i, j int) { h.Elems[i], h.Elems[j] = h.Elems[j], h.Elems[i] }
func (h *IDAccHeap) Push(x ElemHeapIDAcc) {
	h.Elems = append(h.Elems, x)
}

func (h *IDAccHeap) Pop() ElemHeapIDAcc {
	old := h.Elems
	n := len(old)
	x := old[n-1]
	h.Elems = old[0 : n-1]
	return x
}

func InitIDAccHeap(h *IDAccHeap) {
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		downIDAccHeap(h, i, n)
	}
}

func PushIDAccHeap(h *IDAccHeap, x ElemHeapIDAcc) {
	h.Push(x)
	upIDAccHeap(h, h.Len()-1)
}

func PopIDAccHeap(h *IDAccHeap) ElemHeapIDAcc {
	n := h.Len() - 1
	h.Swap(0, n)
	downIDAccHeap(h, 0, n)
	return h.Pop()
}

func RemoveIDAccHeap(h *IDAccHeap, i int) ElemHeapIDAcc {
	n := h.Len() - 1
	if n != i {
		h.Swap(i, n)
		if !downIDAccHeap(h, i, n) {
			upIDAccHeap(h, i)
		}
	}
	return h.Pop()
}

func FixIDAccHeap(h *IDAccHeap, i int) {
	if !downIDAccHeap(h, i, h.Len()) {
		upIDAccHeap(h, i)
	}
}

func upIDAccHeap(h *IDAccHeap, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func downIDAccHeap(h *IDAccHeap, i0, n int) bool {
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
