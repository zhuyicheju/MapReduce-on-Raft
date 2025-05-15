package master

type tasknode struct {
	task_id   int
	timestamp int64
}

type map_heap []tasknode

func (h *map_heap) Len() int { return len(*h) }
func (h *map_heap) Less(i int, j int) bool {
	if (*h)[i].timestamp == (*h)[j].timestamp {
		return (*h)[i].task_id < (*h)[j].task_id
	}
	return (*h)[i].timestamp < (*h)[j].timestamp
}
func (h *map_heap) Swap(i int, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *map_heap) Push(x interface{}) {
	*h = append(*h, x.(tasknode))
}

func (h *map_heap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}
