package dump

var tplCommonData = map[string]interface{}{}

func rdbReveal(path string) map[string]interface{} {
	// deep copy  tplCommonData into data
	data := map[string]interface{}{}
	for key, val := range tplCommonData {
		data[key] = val
	}

	c := counters.Get(path)
	if c == nil {
		return nil
	}
	counter := c.(*Counter)

	data["CurrentInstance"] = path
	data["LargestKeys"] = counter.GetLargestEntries(100)

	largestKeyPrefixesByType := map[string][]*PrefixEntry{}
	for _, entry := range counter.GetLargestKeyPrefixes() {
		// mem use less than 1M, and list is long enough, not necessary to add
		if entry.Bytes < 1000*1000 && len(largestKeyPrefixesByType[entry.Type]) > 50 {
			continue
		}
		largestKeyPrefixesByType[entry.Type] = append(largestKeyPrefixesByType[entry.Type], entry)
	}
	data["LargestKeyPrefixes"] = largestKeyPrefixesByType

	data["TypeBytes"] = counter.typeBytes
	data["TypeNum"] = counter.typeNum
	totleNum := uint64(0)
	for _, v := range counter.typeNum {
		totleNum += v
	}
	totleBytes := uint64(0)
	for _, v := range counter.typeBytes {
		totleBytes += v
	}
	data["TotleNum"] = totleNum
	data["TotleBytes"] = totleBytes

	lenLevelCount := map[string][]*PrefixEntry{}
	for _, entry := range counter.GetLenLevelCount() {
		lenLevelCount[entry.Type] = append(lenLevelCount[entry.Type], entry)
	}
	data["LenLevelCount"] = lenLevelCount
	return data
}
