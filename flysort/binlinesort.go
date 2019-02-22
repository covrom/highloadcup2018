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
	ln4 := len(a) &^ 3
	for i := 0; i < ln4; i += 4 {
		v := a[i]
		aa := a[i+1 : i+4]
		_ = aa[2]
		b0, b1, b2, b3 := v >= num, aa[0] >= num, aa[1] >= num, aa[2] >= num
		var j int8
		switch {
		case b0:
			j = 0
		case b1:
			j = 1
		case b2:
			j = 2
		case b3:
			j = 3
		default:
			continue
		}
		copy(a[j+1:], a[j:])
		a[j] = num
		return a
	}
	for i := ln4; i < len(a); i++ {
		v := a[i]
		if v >= num {
			copy(a[i+1:], a[i:])
			a[i] = num
			return a
		}
	}
	return a
}
