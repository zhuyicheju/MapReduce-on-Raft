package raft

import (
	"container/heap"
)

type MaxHeap []int

func (h MaxHeap) Len() int           { return len(h) }
func (h MaxHeap) Less(i, j int) bool { return h[i] > h[j] } // 注意：我们要使得堆顶最大
func (h MaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MaxHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// 最小堆（高堆）
type MinHeap []int

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i] < h[j] } // 默认堆顶最小
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type MedianTracker struct {
	low      *MaxHeap    // 最大堆（小的一半）
	high     *MinHeap    // 最小堆（大的一半）
	arr      []int       // 存储原始的数组
	arrIndex map[int]int // 记录每个索引的元素值
}

// 从堆中删除元素
func (mt *MedianTracker) removeFromHeap(value int) {
	// 移除元素时需要重新构建堆
	tempHeap := []int{}
	// 选择移除 `low` 还是 `high` 堆中的元素
	if value <= (*mt.low)[0] {
		// 在 low 堆中
		for mt.low.Len() > 0 && (*mt.low)[0] != value {
			tempHeap = append(tempHeap, heap.Pop(mt.low).(int))
		}
		heap.Pop(mt.low)
	} else {
		// 在 high 堆中
		for mt.high.Len() > 0 && (*mt.high)[0] != value {
			tempHeap = append(tempHeap, heap.Pop(mt.high).(int))
		}
		heap.Pop(mt.high)
	}
	// 恢复堆的状态
	for _, num := range tempHeap {
		mt.insertIntoHeap(num)
	}
}

// 将一个新值插入到正确的堆中
func (mt *MedianTracker) insertIntoHeap(value int) {
	if mt.low.Len() == 0 || value <= (*mt.low)[0] {
		heap.Push(mt.low, value)
	} else {
		heap.Push(mt.high, value)
	}
}

// 保持堆的平衡
func (mt *MedianTracker) balanceHeaps() {
	if mt.low.Len() > mt.high.Len()+1 {
		// low 堆比 high 堆多一个元素，移一个元素到 high 堆
		heap.Push(mt.high, heap.Pop(mt.low))
	} else if mt.high.Len() > mt.low.Len() {
		// high 堆比 low 堆多一个元素，移一个元素到 low 堆
		heap.Push(mt.low, heap.Pop(mt.high))
	}
}

// GetMedian 获取当前的中位数
func (mt *MedianTracker) GetMedian() int {
	for i := 0; i <= 1; i++ {
		// log.Printf("%v \n", (*mt.low)[i])
	}
	for i := 0; i < 1; i++ {
		// log.Printf("%v \n", (*mt.high)[i])
	}
	return (*mt.low)[0]
}

// Add 更新数组中索引为 index 的元素，并保持堆的平衡
func (mt *MedianTracker) Add(index, newVal int) {
	// log.Printf("add %v %v\n", index, newVal)
	// 获取旧值
	oldVal := mt.arrIndex[index]
	mt.arrIndex[index] = newVal

	// 删除旧值
	mt.removeFromHeap(oldVal)

	// 插入新值
	mt.insertIntoHeap(newVal)

	// 保持堆的平衡
	mt.balanceHeaps()
}

func NewMedianTracker(arr []int) *MedianTracker {
	low := make(MaxHeap, len(arr)+1/2)
	high := make(MinHeap, len(arr)-(len(arr)+1/2))
	heap.Init(&low)
	heap.Init(&high)

	// 初始将数组的元素加入堆
	mt := &MedianTracker{
		low:      &low,
		high:     &high,
		arr:      arr,
		arrIndex: make(map[int]int),
	}
	for i, num := range arr {
		mt.arrIndex[i] = num
		mt.Add(i, num) // 初始化时也将数组的元素加入堆
	}
	return mt
}
