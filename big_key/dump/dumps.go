package dump

import (
	"container/heap"
	"context"
	"github.com/dongmx/rdb"
	"os"
	"path/filepath"
	"redis_performance_analysis/big_key/decode"
)

/*
// ToCliWriter dump rdb file statistical information to STDOUT.
func ToCliWriter(cli *cli.Context) {
	if cli.NArg() < 1 {
		fmt.Fprintln(cli.App.ErrWriter, " requires at least 1 argument")
		return
	}

	// parse rdbfile
	fmt.Fprintln(cli.App.Writer, "[")
	nargs := cli.NArg()
	for i := 0; i < nargs; i++ {
		file := cli.Args().Get(i)
		decoder := decoder.NewDecoder()
		go Decode(cli, decoder, file)
		cnt := NewCounter()
		cnt.Count(decoder.Entries)
		filename := filepath.Base(file)
		data := getData(filename, cnt)
		data["MemoryUse"] = decoder.GetUsedMem()
		data["CTime"] = decoder.GetTimestamp()
		jsonBytes, _ := json.MarshalIndent(data, "", "    ")
		fmt.Fprint(cli.App.Writer, string(jsonBytes))
		if i == nargs-1 {
			fmt.Fprintln(cli.App.Writer)
		} else {
			fmt.Fprintln(cli.App.Writer, ",")
		}
	}
	fmt.Fprintln(cli.App.Writer, "]")
}*/

// Decode ...
func Decode(ctx context.Context, decoder *decoder.Decoder, file *os.File) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	err := rdb.Decode(file, decoder)
	if err != nil {
		close(decoder.Entries)
		return
	}
}

func getData(filename string, cnt *Counter) map[string]interface{} {
	data := make(map[string]interface{})
	data["CurrentInstance"] = filepath.Base(filename)
	data["LargestKeys"] = cnt.GetLargestEntries(100)
	data["NoExpiryLargestKeys"] = cnt.GetNoExpiryLargestEntries(100)
	data["AllKeyExpiryRange"] = cnt.allKeyExpiryRange

	largestKeyPrefixesByType := map[string][]*PrefixEntry{}
	for _, entry := range cnt.GetLargestKeyPrefixes() {
		// if mem usage is less than 1M, and the list is long enough, then it's unnecessary to add it.
		if entry.Bytes < 1000*1000 && len(largestKeyPrefixesByType[entry.Type]) > 50 {
			continue
		}
		largestKeyPrefixesByType[entry.Type] = append(largestKeyPrefixesByType[entry.Type], entry)
	}
	data["LargestKeyPrefixes"] = largestKeyPrefixesByType
	data["TypeBytes"] = cnt.typeBytes
	data["TypeNum"] = cnt.typeNum
	totalNum := uint64(0)
	for _, v := range cnt.typeNum {
		totalNum += v
	}
	totalBytes := uint64(0)
	for _, v := range cnt.typeBytes {
		totalBytes += v
	}
	data["TotalNum"] = totalNum
	data["TotalBytes"] = totalBytes

	lenLevelCount := map[string][]*PrefixEntry{}
	for _, entry := range cnt.GetLenLevelCount() {
		lenLevelCount[entry.Type] = append(lenLevelCount[entry.Type], entry)
	}
	data["LenLevelCount"] = lenLevelCount

	var slotBytesHeap SlotHeap
	for slot, length := range cnt.slotBytes {
		heap.Push(&slotBytesHeap, &SlotEntry{
			Slot: slot, Size: length,
		})
	}

	var slotSizeHeap SlotHeap
	for slot, size := range cnt.slotNum {
		heap.Push(&slotSizeHeap, &SlotEntry{
			Slot: slot, Size: size,
		})
	}

	topN := 100
	slotBytes := make(SlotHeap, 0, topN)
	slotNums := make(SlotHeap, 0, topN)

	for i := 0; i < topN; i++ {
		continueFlag := false
		if slotBytesHeap.Len() > 0 {
			continueFlag = true
			slotBytes = append(slotBytes, heap.Pop(&slotBytesHeap).(*SlotEntry))
		}
		if slotSizeHeap.Len() > 0 {
			continueFlag = true
			slotNums = append(slotNums, heap.Pop(&slotSizeHeap).(*SlotEntry))
		}

		if !continueFlag {
			break
		}
	}

	data["SlotBytes"] = slotBytes
	data["SlotNums"] = slotNums

	return data
}

const (
	lessOrEq1Day  = "lessOrEq1Day"
	lessOrEq3Day  = "lessOrEq3Day"
	lessOrEq7Day  = "lessOrEq7Day"
	lessOrEq14Day = "lessOrEq14Day"
	lessOrEq30Day = "lessOrEq30Day"
	lessOrEq90Day = "lessOrEq90Day"
	gt90Day       = "gt90Day"
	noExpiry      = "noExpiry"
)

// calcKeyExpiryRange
// @expiry key expiry time
// @ctime  ctime  rdb create time
func calcKeyExpiryRange(expiry, ctime int64) string {
	if expiry == 0 {
		return noExpiry
	}
	expiryDiff := (expiry - ctime*1000) / 1000
	daySeconds := int64(24 * 60 * 60)
	switch {
	case expiryDiff <= daySeconds:
		return lessOrEq1Day
	case expiryDiff > daySeconds && expiryDiff <= 3*daySeconds:
		return lessOrEq3Day
	case expiryDiff > 3*daySeconds && expiryDiff <= 7*daySeconds:
		return lessOrEq7Day
	case expiryDiff > 7*daySeconds && expiryDiff <= 14*daySeconds:
		return lessOrEq14Day
	case expiryDiff > 14*daySeconds && expiryDiff <= 30*daySeconds:
		return lessOrEq30Day
	case expiryDiff > 30*daySeconds && expiryDiff <= 90*daySeconds:
		return lessOrEq90Day
	default:
		return gt90Day
	}

}
