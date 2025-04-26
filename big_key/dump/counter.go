package dump

import (
	"container/heap"
	"redis_performance_analysis/big_key/decode"
	"sort"
	"strconv"
	"strings"
)

// NewCounter return a pointer of Counter
func NewCounter() *Counter {
	h := &entryHeap{}
	heap.Init(h)
	p := &prefixHeap{}
	heap.Init(p)
	u := &entryHeap{}
	heap.Init(u)
	return &Counter{
		largestEntries:       h,
		largestKeyPrefixes:   p,
		unExpiryKeyEntries:   u,
		lengthLevel0:         100,
		lengthLevel1:         1000,
		lengthLevel2:         10000,
		lengthLevel3:         100000,
		lengthLevel4:         1000000,
		lengthLevelBytes:     map[typeKey]uint64{},
		lengthLevelNum:       map[typeKey]uint64{},
		keyPrefixBytes:       map[typeKey]uint64{},
		keyPrefixNum:         map[typeKey]uint64{},
		keyPrefixExpiryRange: map[typeKey]map[string]uint64{},
		allKeyExpiryRange:    map[string]uint64{},
		typeBytes:            map[string]uint64{},
		typeNum:              map[string]uint64{},
		separators:           ":;,_- ",
		slotBytes:            map[int]uint64{},
		slotNum:              map[int]uint64{},
	}
}

// Counter for redis memory useage
type Counter struct {
	largestEntries       *entryHeap
	largestKeyPrefixes   *prefixHeap
	unExpiryKeyEntries   *entryHeap
	lengthLevel0         uint64
	lengthLevel1         uint64
	lengthLevel2         uint64
	lengthLevel3         uint64
	lengthLevel4         uint64
	lengthLevelBytes     map[typeKey]uint64
	lengthLevelNum       map[typeKey]uint64
	keyPrefixBytes       map[typeKey]uint64
	keyPrefixNum         map[typeKey]uint64
	keyPrefixExpiryRange map[typeKey]map[string]uint64
	allKeyExpiryRange    map[string]uint64
	separators           string
	typeBytes            map[string]uint64
	typeNum              map[string]uint64
	slotBytes            map[int]uint64
	slotNum              map[int]uint64
	ctime                int64 // 创建快照的时间
}

// Count by various dimensions
func (c *Counter) Count(decoder *decoder.Decoder) {
	for e := range decoder.Entries {
		if c.ctime == 0 {
			c.ctime = decoder.GetTimestamp()
		}
		c.count(e)
	}
	// get largest prefixes
	c.calcuLargestKeyPrefix(1000)
}

// GetLargestEntries from heap, num max is 500
func (c *Counter) GetLargestEntries(num int) []*decoder.Entry {
	var res []*decoder.Entry

	// get a copy of c.largestEntries
	for i := 0; i < c.largestEntries.Len(); i++ {
		entries := *c.largestEntries
		res = append(res, entries[i])
	}
	sort.Sort(sort.Reverse(entryHeap(res)))
	if num < len(res) {
		res = res[:num]
	}
	return res
}

// GetNoExpiryLargestEntries from heap, num max is 500
func (c *Counter) GetNoExpiryLargestEntries(num int) []*decoder.Entry {
	var res []*decoder.Entry

	// get a copy of c.largestEntries
	for i := 0; i < c.unExpiryKeyEntries.Len(); i++ {
		entries := *c.unExpiryKeyEntries
		res = append(res, entries[i])
	}
	sort.Sort(sort.Reverse(entryHeap(res)))
	if num < len(res) {
		res = res[:num]
	}
	return res
}

// GetLargestKeyPrefixes from heap
func (c *Counter) GetLargestKeyPrefixes() []*PrefixEntry {
	var res []*PrefixEntry

	// get a copy of c.largestKeyPrefixes
	entries := *c.largestKeyPrefixes
	for i := 0; i < c.largestKeyPrefixes.Len(); i++ {
		res = append(res, entries[i])
	}
	sort.Sort(sort.Reverse(prefixHeap(res)))
	return res
}

// GetLenLevelCount from map
func (c *Counter) GetLenLevelCount() []*PrefixEntry {
	var res []*PrefixEntry

	// get a copy of lengthLevelBytes and lengthLevelNum
	for key := range c.lengthLevelBytes {
		entry := &PrefixEntry{}
		entry.Type = key.Type
		entry.Key = key.Key
		entry.Bytes = c.lengthLevelBytes[key]
		entry.Num = c.lengthLevelNum[key]
		res = append(res, entry)
	}
	return res
}

func (c *Counter) count(e *decoder.Entry) {
	c.countLargestEntries(e, 500)
	c.countByType(e)
	c.countByLength(e)
	c.countByKeyPrefix(e)
	c.countBySlot(e)
	c.countUnExpiryEntries(e, 500)
	c.countAllEntriesExpiryRange(e)
}

func (c *Counter) countLargestEntries(e *decoder.Entry, num int) {
	heap.Push(c.largestEntries, e)
	l := c.largestEntries.Len()
	if l > num {
		heap.Pop(c.largestEntries)
	}
}

func (c *Counter) countAllEntriesExpiryRange(e *decoder.Entry) {
	c.allKeyExpiryRange[calcKeyExpiryRange(e.Expiry, c.ctime)]++
}

func (c *Counter) countUnExpiryEntries(e *decoder.Entry, num int) {
	if e.Expiry == 0 {
		heap.Push(c.unExpiryKeyEntries, e)
		l := c.largestEntries.Len()
		if l > num {
			heap.Pop(c.unExpiryKeyEntries)
		}
	}
}

func (c *Counter) countByLength(e *decoder.Entry) {
	key := typeKey{
		Type: e.Type,
		Key:  strconv.FormatUint(c.lengthLevel0, 10),
	}

	add := func(c *Counter, key typeKey, e *decoder.Entry) {
		c.lengthLevelBytes[key] += e.Bytes
		c.lengthLevelNum[key]++
	}

	// must lengthLevel4 > lengthLevel3 > lengthLevel2 ...
	if e.NumOfElem > c.lengthLevel4 {
		key.Key = strconv.FormatUint(c.lengthLevel4, 10)
		add(c, key, e)
	} else if e.NumOfElem > c.lengthLevel3 {
		key.Key = strconv.FormatUint(c.lengthLevel3, 10)
		add(c, key, e)
	} else if e.NumOfElem > c.lengthLevel2 {
		key.Key = strconv.FormatUint(c.lengthLevel2, 10)
		add(c, key, e)
	} else if e.NumOfElem > c.lengthLevel1 {
		key.Key = strconv.FormatUint(c.lengthLevel1, 10)
		add(c, key, e)
	} else if e.NumOfElem > c.lengthLevel0 {
		key.Key = strconv.FormatUint(c.lengthLevel0, 10)
		add(c, key, e)
	}
}

func (c *Counter) countByType(e *decoder.Entry) {
	c.typeNum[e.Type]++
	c.typeBytes[e.Type] += e.Bytes
}

func (c *Counter) countByKeyPrefix(e *decoder.Entry) {
	// reset all numbers to 0
	k := strings.Map(func(c rune) rune {
		/*if c >= 48 && c <= 57 { //48 == "0" 57 == "9"
			return '0'
		}*/
		return c
	}, e.Key)
	prefixes := getPrefixes(k, c.separators)
	key := typeKey{
		Type: e.Type,
	}
	for _, prefix := range prefixes {
		if len(prefix) == 0 {
			continue
		}
		key.Key = prefix
		c.keyPrefixBytes[key] += e.Bytes
		c.keyPrefixNum[key]++

		expiryRange := calcKeyExpiryRange(e.Expiry, c.ctime)
		if _, ok := c.keyPrefixExpiryRange[key]; !ok {
			c.keyPrefixExpiryRange[key] = map[string]uint64{expiryRange: 1}
		} else {
			c.keyPrefixExpiryRange[key][expiryRange]++
		}
	}
}

func (c *Counter) countBySlot(e *decoder.Entry) {
	if len(e.Key) > 0 {
		slot := Slot(e.Key)

		c.slotNum[slot]++
		c.slotBytes[slot] += e.Bytes
	}
}

func (c *Counter) calcuLargestKeyPrefix(num int) {
	for key := range c.keyPrefixBytes {
		k := &PrefixEntry{
			ExpiryRange: map[string]uint64{},
		}
		k.Type = key.Type
		k.Key = key.Key
		k.Bytes = c.keyPrefixBytes[key]
		k.Num = c.keyPrefixNum[key]
		k.ExpiryRange = c.keyPrefixExpiryRange[key]

		delete(c.keyPrefixBytes, key)
		delete(c.keyPrefixNum, key)
		delete(c.keyPrefixExpiryRange, key)

		heap.Push(c.largestKeyPrefixes, k)
		l := c.largestKeyPrefixes.Len()
		if l > num {
			heap.Pop(c.largestKeyPrefixes)
		}
	}
}

type entryHeap []*decoder.Entry

func (h entryHeap) Len() int {
	return len(h)
}
func (h entryHeap) Less(i, j int) bool {
	return h[i].Bytes < h[j].Bytes
}
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *entryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *entryHeap) Push(e interface{}) {
	*h = append(*h, e.(*decoder.Entry))
}

type typeKey struct {
	Type string
	Key  string
}

type prefixHeap []*PrefixEntry

// PrefixEntry record value by prefix
type PrefixEntry struct {
	typeKey
	Bytes       uint64
	Num         uint64
	ExpiryRange map[string]uint64
}

func (h prefixHeap) Len() int {
	return len(h)
}
func (h prefixHeap) Less(i, j int) bool {
	if h[i].Bytes < h[j].Bytes {
		return true
	} else if h[i].Bytes == h[j].Bytes {
		if h[i].Num < h[j].Num {
			return true
		} else if h[i].Num == h[j].Num {
			if h[i].Key > h[j].Key {
				return true
			}
		}
	}
	return false

}
func (h prefixHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *prefixHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *prefixHeap) Push(k interface{}) {
	*h = append(*h, k.(*PrefixEntry))
}

func appendIfMissing(slice []int, i int) []int {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

func removeDuplicatesUnordered(elements []string) []string {
	encountered := map[string]bool{}

	// Create a map of all unique elements.
	for v := range elements {
		encountered[elements[v]] = true
	}

	// Place all keys from the map into a slice.
	var result []string
	for key := range encountered {
		result = append(result, key)
	}
	return result
}

func getPrefixes(s, sep string) []string {
	var res []string
	sepIdx := strings.IndexAny(s, sep)
	if sepIdx < 0 {
		res = append(res, s)
	}
	for sepIdx > -1 {
		r := s[:sepIdx+1]
		if len(res) > 0 {
			r = res[len(res)-1] + s[:sepIdx+1]
		}
		res = append(res, r)
		s = s[sepIdx+1:]
		sepIdx = strings.IndexAny(s, sep)
	}
	// Trim all suffix of separators
	for i := range res {
		for hasAnySuffix(res[i], sep) {
			res[i] = res[i][:len(res[i])-1]
		}
	}
	res = removeDuplicatesUnordered(res)
	return res
}

func hasAnySuffix(s, suffix string) bool {
	for _, c := range suffix {
		if strings.HasSuffix(s, string(c)) {
			return true
		}
	}
	return false
}

// SlotEntry support for sorting of slots
type SlotEntry struct {
	Slot int
	Size uint64
}

type SlotHeap []*SlotEntry

func (h SlotHeap) Len() int {
	return len(h)
}
func (h SlotHeap) Less(i, j int) bool {
	return h[i].Size > h[j].Size
}
func (h SlotHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *SlotHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *SlotHeap) Push(e interface{}) {
	*h = append(*h, e.(*SlotEntry))
}
