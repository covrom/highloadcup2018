package flysort

const heap_workers = 3

func ParallelHeapSort(ints []int) []int {
	chres := make(chan int, 100)
	chouts := make([]chan int, heap_workers)
	chint := make(chan int, heap_workers*10)
	for i := 0; i < heap_workers; i++ {
		chout := make(chan int, heap_workers*10)
		chouts[i] = chout
		go heapSort(chint, chout, len(ints)/heap_workers+1)
	}
	go heapCollect(chouts, chres, len(ints))
	go func() {
		for _, v := range ints {
			chint <- v
		}
		close(chint)
	}()

	res := make([]int, 0, len(ints))
	for v := range chres {
		res = append(res, v)
		if len(res) == len(ints) {
			break
		}
	}
	return res
}

func heapSort(chint, chout chan int, bufSize int) {
	h := make(heapInts, 0, bufSize)
	for v := range chint {
		PushHeap(&h, v)
	}
	for len(h) > 0 {
		chout <- PopHeap(&h)
	}
	close(chout)
}

func heapCollect(chouts []chan int, chres chan int, bufSize int) {
	h := make(heapInts, 0, bufSize)
loop:
	for {
		for _, chout := range chouts {
			select {
			case v := <-chout:
				PushHeap(&h, v)
				bufSize--
				if bufSize <= 0 {
					break loop
				}
			default:
			}
		}
	}
	for len(h) > 0 {
		chres <- PopHeap(&h)
	}
	close(chres)
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
