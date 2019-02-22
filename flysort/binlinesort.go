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
		copy(a[i+int(j)+1:], a[i+int(j):])
		a[i+int(j)] = num
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

func BubbleInsert(a heapInts, num int) heapInts {
	if len(a) < heapLimit {
		a = append(a, num)
	} else {
		if a[len(a)-1] < num {
			return a
		}
		a[len(a)-1] = num
	}
	i := len(a) - 1
	for i >= 0 {
		f := -1
		for j := i - 8; j < i; j++ {
			if j >= 0 {
				if a[j] > num {
					f = j
					break
				}
			}
		}
		if f >= 0 {
			switch i - f {
			case 1:
				a[f+1] = a[f]
			case 2:
				v1 := a[f]
				v2 := a[f+1]
				a[f+1] = v1
				a[f+2] = v2
			case 3:
				v1 := a[f]
				v2 := a[f+1]
				v3 := a[f+2]
				a[f+1] = v1
				a[f+2] = v2
				a[f+3] = v3
			case 4:
				v1 := a[f]
				v2 := a[f+1]
				v3 := a[f+2]
				v4 := a[f+3]
				a[f+1] = v1
				a[f+2] = v2
				a[f+3] = v3
				a[f+4] = v4
			case 5:
				v1 := a[f]
				v2 := a[f+1]
				v3 := a[f+2]
				v4 := a[f+3]
				v5 := a[f+4]
				a[f+1] = v1
				a[f+2] = v2
				a[f+3] = v3
				a[f+4] = v4
				a[f+5] = v5
			case 6:
				v1 := a[f]
				v2 := a[f+1]
				v3 := a[f+2]
				v4 := a[f+3]
				v5 := a[f+4]
				v6 := a[f+5]
				a[f+1] = v1
				a[f+2] = v2
				a[f+3] = v3
				a[f+4] = v4
				a[f+5] = v5
				a[f+6] = v6
			case 7:
				v1 := a[f]
				v2 := a[f+1]
				v3 := a[f+2]
				v4 := a[f+3]
				v5 := a[f+4]
				v6 := a[f+5]
				v7 := a[f+6]
				a[f+1] = v1
				a[f+2] = v2
				a[f+3] = v3
				a[f+4] = v4
				a[f+5] = v5
				a[f+6] = v6
				a[f+7] = v7
			case 8:
				v1 := a[f]
				v2 := a[f+1]
				v3 := a[f+2]
				v4 := a[f+3]
				v5 := a[f+4]
				v6 := a[f+5]
				v7 := a[f+6]
				v8 := a[f+7]
				a[f+1] = v1
				a[f+2] = v2
				a[f+3] = v3
				a[f+4] = v4
				a[f+5] = v5
				a[f+6] = v6
				a[f+7] = v7
				a[f+8] = v8
			}
			a[f] = num
		}
		i -= 8
	}
	return a
}
