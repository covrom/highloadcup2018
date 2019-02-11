package avltree

type IDInt32 struct {
	ID  uint32
	Val int32
	tl  tl `avlgen:"TreeIDInt32,cmp:cmpiv,cmpval:cmpk(uint32),iter,export"`
}

func (a *IDInt32) cmpiv(b *IDInt32) (bool, bool) {
	return a.ID == b.ID, a.ID > b.ID
}

func (a *IDInt32) cmpk(b uint32) (bool, bool) {
	return a.ID == b, a.ID > b
}

func btoi(a bool) int {
	// See: https://github.com/golang/go/issues/6011#issuecomment-254303032
	//
	// Hopefully this will make the generated code suck less in the
	// future because currently it's comically bad. false is branch
	// predicted by the compiler to be unlikely, it's put after the
	// function, jumps back, then the compiler forgets that it just
	// loaded 0 or 1 into a register and does a bounds check on a 2
	// element array.
	x := 0
	if a {
		x = 1
	}
	// By performing this '& 1' we add one useless instruction but
	// we eliminate the bounds check which is a branch.
	// Yes, it is worth it.
	return x & 1
}

type tl struct {
	nodes  [2]TreeIDInt32
	height int
}

type TreeIDInt32 struct {
	n    *IDInt32
	size int // Total number of keys in the tree
}

func (tr *TreeIDInt32) height() int {
	if tr.n == nil {
		return 0
	}
	return tr.n.tl.height
}

func (tr *TreeIDInt32) reheight() {
	l := tr.n.tl.nodes[0].height()
	r := tr.n.tl.nodes[1].height()
	if l > r {
		tr.n.tl.height = l + 1
	} else {
		tr.n.tl.height = r + 1
	}
}

func (tr *TreeIDInt32) rebalance() {
	lh := tr.n.tl.nodes[0].height()
	rh := tr.n.tl.nodes[1].height()
	if lh > rh {
		tr.n.tl.height = lh + 1
		if lh-rh < 2 {
			return
		}
		child := &tr.n.tl.nodes[0]
		if child.n.tl.nodes[0].height() < child.n.tl.nodes[1].height() {
			pivot := child.n.tl.nodes[1].n
			child.n.tl.nodes[1].n = pivot.tl.nodes[0].n
			pivot.tl.nodes[0].n = child.n
			pivot.tl.nodes[0].reheight()
			child.n = pivot
			child.reheight()
		}
		pivot := child.n
		tr.n.tl.nodes[0].n = pivot.tl.nodes[1].n
		pivot.tl.nodes[1].n = tr.n
		pivot.tl.nodes[1].reheight()
		tr.n = pivot
		tr.reheight()
	} else {
		tr.n.tl.height = rh + 1
		if rh-lh < 2 {
			return
		}
		child := &tr.n.tl.nodes[1]
		if child.n.tl.nodes[1].height() < child.n.tl.nodes[0].height() {
			pivot := child.n.tl.nodes[0].n
			child.n.tl.nodes[0].n = pivot.tl.nodes[1].n
			pivot.tl.nodes[1].n = child.n
			pivot.tl.nodes[1].reheight()
			child.n = pivot
			child.reheight()
		}
		pivot := child.n
		tr.n.tl.nodes[1].n = pivot.tl.nodes[0].n
		pivot.tl.nodes[0].n = tr.n
		pivot.tl.nodes[0].reheight()
		tr.n = pivot
		tr.reheight()
	}
}

// Empty returns true if tree does not contain any nodes.
func (tr *TreeIDInt32) Empty() bool {
	return tr.size == 0
}

// Size returns the number of elements stored in the tree.
func (tr *TreeIDInt32) Size() int {
	return tr.size
}

func (tr *TreeIDInt32) Insert(x *IDInt32) {
	path := [64]*TreeIDInt32{}
	depth := 0
	for tr.n != nil {
		path[depth] = tr
		depth++
		_, less := tr.n.cmpiv(x)
		/*
		 * We need to decide how to handle equality.
		 *
		 * Four options:
		 * 1. Silently assume it doesn't happen, just insert
		 *    duplicate elements. It's your foot and your
		 *    trigger. (current choice)
		 * 2. Silently ignore and don't insert.
		 * 3. Refuse to insert, return boolean for success.
		 * 4. Replace, return old element.
		 */
		tr = &tr.n.tl.nodes[btoi(!less)]
	}
	x.tl.nodes[0].n = nil
	x.tl.nodes[1].n = nil
	x.tl.height = 1
	tr.n = x

	for i := depth - 1; i >= 0; i-- {
		path[i].rebalance()
	}
	tr.size++
}

func (tr *TreeIDInt32) Delete(x *IDInt32) {
	/*
	 * We silently ignore deletions of elements that are
	 * not in the tree. The options here are to return
	 * something or panic or do nothing. All three equally
	 * valid.
	 */
	if tr.n == nil {
		return
	}

	if tr.n == x {
		if tr.n.tl.nodes[0].n == nil {
			tr.n = tr.n.tl.nodes[1].n
		} else if tr.n.tl.nodes[1].n == nil {
			tr.n = tr.n.tl.nodes[0].n
		} else {
			next := tr.n.tl.nodes[0].First()
			tr.n.tl.nodes[0].Delete(next)
			next.tl = tr.n.tl
			tr.n = next
			tr.rebalance()
		}
	} else {
		_, less := tr.n.cmpiv(x)
		tr.n.tl.nodes[btoi(!less)].Delete(x)
		tr.rebalance()
	}
	tr.size--
}

func (tr *TreeIDInt32) Lookup(x *IDInt32) *IDInt32 {
	n := tr.n

	for n != nil {
		eq, less := n.cmpiv(x)
		if eq {
			break
		}
		n = n.tl.nodes[btoi(!less)].n
	}
	return n
}

func (tr *TreeIDInt32) Last() (ret *IDInt32) {
	for n := tr.n; n != nil; n = n.tl.nodes[0].n {
		ret = n
	}
	return
}

func (tr *TreeIDInt32) First() (ret *IDInt32) {
	for n := tr.n; n != nil; n = n.tl.nodes[1].n {
		ret = n
	}
	return
}

func (tr *TreeIDInt32) LookupVal(x uint32) *IDInt32 {
	n := tr.n
	for n != nil {
		eq, less := n.cmpk(x)
		if eq {
			break
		}
		n = n.tl.nodes[btoi(!less)].n
	}
	return n
}

// Find nearest value greater than or equal to x
func (tr *TreeIDInt32) SearchValGEQ(x uint32) *IDInt32 {
	// Empty tree can't match.
	if tr.n == nil {
		return nil
	}
	eq, less := tr.n.cmpk(x)
	if eq {
		return tr.n
	}
	if !less {
		l := tr.n.tl.nodes[1].SearchValGEQ(x)
		if l != nil {
			_, less := tr.n.cmpiv(l)
			if !less {
				return l
			}
		}
		return tr.n
	}
	return tr.n.tl.nodes[0].SearchValGEQ(x)
}

// Find nearest value less than or equal to x
func (tr *TreeIDInt32) SearchValLEQ(x uint32) *IDInt32 {
	// Empty tree can't match.
	if tr.n == nil {
		return nil
	}
	eq, less := tr.n.cmpk(x)
	if eq {
		return tr.n
	}
	if less {
		l := tr.n.tl.nodes[0].SearchValLEQ(x)
		if l != nil {
			_, less := tr.n.cmpiv(l)
			if less {
				return l
			}
		}
		return tr.n
	}
	return tr.n.tl.nodes[1].SearchValLEQ(x)
}

func (tr *TreeIDInt32) DeleteVal(x uint32) {
	/*
	 * We silently ignore deletions of elements that are
	 * not in the tree. The options here are to return
	 * something or panic or do nothing. All three equally
	 * valid.
	 */
	if tr.n == nil {
		return
	}

	eq, more := tr.n.cmpk(x)
	if eq {
		if tr.n.tl.nodes[0].n == nil {
			tr.n = tr.n.tl.nodes[1].n
		} else if tr.n.tl.nodes[1].n == nil {
			tr.n = tr.n.tl.nodes[0].n
		} else {
			next := tr.n.tl.nodes[0].First()
			tr.n.tl.nodes[0].Delete(next)
			next.tl = tr.n.tl
			tr.n = next
			tr.rebalance()
		}
	} else {
		tr.n.tl.nodes[btoi(!more)].DeleteVal(x)
		tr.rebalance()
	}
	tr.size--
}

type TreeIDInt32Iter struct {
	// First and last elements of the iterator
	start, end *IDInt32
	// Should start and end elements be included in the iteration?
	incs, ince, rev bool
	// The path we took to reach the previous element.
	path []*TreeIDInt32
}

func (tr *TreeIDInt32) Iter(start, end *IDInt32, incs, ince bool) *TreeIDInt32Iter {
	it := &TreeIDInt32Iter{start: start, end: end, incs: incs, ince: ince, path: make([]*TreeIDInt32, 0, tr.height())}
	if start != nil {
		it.findStartPath(tr)
	} else {
		it.diveDown(tr)
	}
	if end == nil {
		it.end = tr.Last()
	}
	// Explicitly handle start == end.
	if it.start == it.end && it.incs != it.ince {
		// one false means both false
		it.incs = false
		it.ince = false
	}
	eq, less := it.start.cmpiv(it.end)
	it.rev = !less && !eq
	return it
}

// start, end - start and end values of iteration.
// edgeStart,edgeEnd - ignore start/end and start/end the iteration at the edge of the tree.
// incs, ince - include the start/end value in the iteration.
func (tr *TreeIDInt32) IterVal(start, end uint32, edgeStart, edgeEnd, incs, ince bool) *TreeIDInt32Iter {
	var s, e *IDInt32
	if !edgeStart {
		s = tr.SearchValLEQ(start)
		if eq, _ := s.cmpk(start); !eq {
			// If we got a value less than start,
			// force incs to false since we don't
			// want to include it.
			incs = false
		}
	}
	if !edgeEnd {
		e = tr.SearchValGEQ(end)
		if eq, _ := e.cmpk(end); !eq {
			// If we got a value greater than end,
			// force ince to false since we don't
			// want to include it.
			ince = false
		}
	}
	return tr.Iter(s, e, incs, ince)
}

// Helper function, don't use.
func (it *TreeIDInt32Iter) diveDown(t *TreeIDInt32) {
	for t.n != nil {
		it.path = append(it.path, t)
		it.start = t.n // lazy, should just be done once.
		t = &t.n.tl.nodes[btoi(!it.rev)]
	}
}

// Helper function, don't use.
func (it *TreeIDInt32Iter) findStartPath(t *TreeIDInt32) {
	for {
		it.path = append(it.path, t)
		eq, less := t.n.cmpiv(it.start)
		if eq {
			break
		}
		t = &t.n.tl.nodes[btoi(!less)]
	}
}

func (it *TreeIDInt32Iter) Value() *IDInt32 {
	return it.start
}

func (it *TreeIDInt32Iter) Next() bool {
	if it.start != it.end {
		// incs can only be set for the first element of the iterator,
		// if it is, we just don't move to the next element.
		if it.incs {
			it.incs = false
			return true
		}
		/*
		 * right - towards the end of iteration (0 in forward iteration)
		 * left - towards beginning of the iteration (1 in forward iteration)
		 *
		 * Last returned element is it.start
		 * We got it through t := it.path[len(it.path)-1].
		 * if t has a tree to the right, the next element
		 * is the leftmost element of the right tree.
		 * If it doesn't, the next element is the one parent
		 * we have that's bigger than us.
		 *
		 * We don't check for underflow of path. If that
		 * happens something is already seriously wrong,
		 * crashing is the best option.
		 */
		if it.start.tl.nodes[btoi(it.rev)].n != nil {
			it.diveDown(&it.start.tl.nodes[btoi(it.rev)])
		} else {
			for {
				it.path = it.path[:len(it.path)-1]
				_, less := it.path[len(it.path)-1].n.cmpiv(it.start)
				if less == it.rev {
					break
				}
			}
			it.start = it.path[len(it.path)-1].n
		}
	}
	if it.start != it.end {
		return true
	} else if it.ince {
		it.ince = false
		return it.end != nil // can happen with empty iterator.
	} else {
		return false
	}
}
