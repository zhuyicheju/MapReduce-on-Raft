type ByKey []KeyValue
type MinHeap []Merge
type KeyValue struct {
	Key   string
	Value string
}

type Merge struct {
	kv    KeyValue
	index int
}

func (h *MinHeap) Len() int               { return len(*h) }
func (h *MinHeap) Less(i int, j int) bool { return (*h)[i].kv.Key < (*h)[j].kv.Key }
func (h *MinHeap) Swap(i int, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(Merge))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}