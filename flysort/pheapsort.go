package flysort

const heap_workers = 3

func ParallelHeapSort(ints []int) []int {
	chres := make(chan int, heap_workers*3)
	workSize := len(ints) / heap_workers
	for i := 0; i < len(ints); i += workSize {
		right := i + workSize
		if right >= len(ints) {
			right = len(ints)
		}
		go heapSort(ints[i:right], chres)
	}
	h := make(heapInts, 0, len(ints))
	heapLimit = 0
	for v := range chres {
		PushHeap(&h, v)
		if len(h) == len(ints) {
			break
		}
	}
	res := make([]int, len(h))
	for i := range res {
		res[i] = PopHeap(&h)
	}
	return res
}

func heapSort(ints []int, chout chan int) {
	h := make(heapInts, 0, len(ints))
	for _, v := range ints {
		PushHeap(&h, v)
	}
	for len(h) > 0 {
		chout <- PopHeap(&h)
	}
}

func NormalHeapSort(ints []int) []int {
	h := make(heapInts, 0, len(ints))
	for _, v := range ints {
		PushHeap(&h, v)
	}
	res := make([]int, 0, len(h))
	for len(h) > 0 {
		res = append(res, PopHeap(&h))
	}
	return res
}
