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
		if i+3 < len(a) {
			aa := a[i+1 : i+4]
			_ = aa[2]
			b0, b1, b2, b3 := v >= num, aa[0] >= num, aa[1] >= num, aa[2] >= num
			switch {
			case b0:
				copy(a[i+1:], a[i:])
				a[i] = num
				return a
			case b1:
				copy(a[i+2:], a[i+1:])
				a[i+1] = num
				return a
			case b2:
				copy(a[i+3:], a[i+2:])
				a[i+2] = num
				return a
			case b3:
				copy(a[i+4:], a[i+3:])
				a[i+3] = num
				return a
			}
			i += 3
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
