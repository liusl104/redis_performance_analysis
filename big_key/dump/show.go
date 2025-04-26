package dump

import (
	"context"
	"os"
	"redis_performance_analysis/big_key/decode"
)

var counters = NewSafeMap()

/*
func listPathFiles(pathname string) []string {
	var filenames []string
	fi, err := os.Lstat(pathname) // For read access.
	if err != nil {
		return filenames
	}
	if fi.IsDir() {
		files, err := io.ReadDir(pathname)
		if err != nil {
			log.Fatal(err)
		}
		for _, f := range files {
			name := path.Join(pathname, f.Name())
			filenames = append(filenames, name)
		}
	} else {
		filenames = append(filenames, pathname)
	}
	return filenames
}*/

func openRdb(filepath string) (*os.File, error) {
	_, err := os.Stat(filepath)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Show parse rdb file(s)
func Show(ctx context.Context, fileName string) (map[string]interface{}, error) {
	var data map[string]interface{}
	file, err := openRdb(fileName)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return nil, err
	}
	/*if !counters.Check(fileName) {*/
	d := decoder.NewDecoder()
	// 解析rdb文件
	go Decode(ctx, d, file)
	counter := NewCounter()
	counter.Count(d)
	// counters.Set(fileName, counter)
	data = getData(fileName, counter)
	data["MemoryUse"] = d.GetUsedMem()
	data["CTime"] = d.GetTimestamp()
	// 释放
	// counters.Delete(fileName)
	_, isOpen := <-d.Entries
	if isOpen {
		close(d.Entries)
	}
	/*} else {
		return nil, errors.New(fmt.Sprintf("Lock file %s fail", fileName))
	}*/

	return data, nil
}
