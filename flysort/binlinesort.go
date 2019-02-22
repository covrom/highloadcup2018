package flysort

func SortInsert(a heapInts, num int) heapInts {
	n := len(a)
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		if a[h] < num {
			i = h + 1
		} else {
			j = h
		}
	}
	if n < heapLimit {
		a = append(a, num)
	}
	if i < n {
		copy(a[i+1:], a[i:])
		a[i] = num
	}
	return a
}

func LineInsert(a heapInts, num int) heapInts {
	if len(a) < heapLimit {
		a = append(a, num)
	}
	for i, v := range a {
		if v >= num {
			copy(a[i+1:], a[i:])
			a[i] = num
			return a
		}
	}
	return a
}

func LineUnrollInsert(a heapInts, num int) heapInts {
	if len(a) < heapLimit {
		a = append(a, num)
	}
	for i := 0; i < len(a); i++ {
		v := a[i]
		if i+4 < len(a) {
			if v >= num {
				copy(a[i+1:], a[i:])
				a[i] = num
				return a
			}
			i++
			if a[i] >= num {
				copy(a[i+1:], a[i:])
				a[i] = num
				return a
			}
			i++
			if a[i] >= num {
				copy(a[i+1:], a[i:])
				a[i] = num
				return a
			}
			i++
			if a[i] >= num {
				copy(a[i+1:], a[i:])
				a[i] = num
				return a
			}
		} else {
			if v >= num {
				copy(a[i+1:], a[i:])
				a[i] = num
				return a
			}
		}
	}
	return a
}
