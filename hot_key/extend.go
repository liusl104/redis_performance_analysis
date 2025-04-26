package hotkeys

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func hasAnySuffix(s, suffix string) bool {
	for _, c := range suffix {
		if strings.HasSuffix(s, string(c)) {
			return true
		}
	}
	return false
}

/*
	func isChanClose(ch chan NetPacket) bool {
		select {
		case _, received := <-ch:
			return !received
		default:
		}
		return false
	}
*/
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

func InSlice(items []string, item string) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

func Split(s, sep string) []string {
	return strings.Split(s, sep)
}

func Struct2MapByTag(s interface{}, tagName string) map[string]interface{} {
	t := reflect.TypeOf(s)
	v := reflect.ValueOf(s)

	if v.Kind() == reflect.Ptr && v.Elem().Kind() == reflect.Struct {
		t = t.Elem()
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil
	}

	m := make(map[string]interface{})

	for i := 0; i < t.NumField(); i++ {
		fv := v.Field(i)
		ft := t.Field(i)

		if !fv.CanInterface() {
			continue
		}

		if ft.PkgPath != "" { // unexported
			continue
		}

		var name string
		var option string
		tag := ft.Tag.Get(tagName)
		if tag != "" {
			ts := strings.Split(tag, ",")
			if len(ts) == 1 {
				name = ts[0]
			} else if len(ts) > 1 {
				name = ts[0]
				option = ts[1]
			}
			if name == "-" {
				continue // skip this field
			}
			if name == "" {
				name = strings.ToLower(ft.Name)
			}
			if option == "omitempty" {
				if isEmpty(&fv) {
					continue // skip empty field
				}
			}
		} else {
			name = strings.ToLower(ft.Name)
		}

		if ft.Anonymous && fv.Kind() == reflect.Ptr && fv.IsNil() {
			continue
		}
		if (ft.Anonymous && fv.Kind() == reflect.Struct) ||
			(ft.Anonymous && fv.Kind() == reflect.Ptr && fv.Elem().Kind() == reflect.Struct) {

			// embedded struct
			embedded := Struct2MapByTag(fv.Interface(), tagName)
			for embName, embValue := range embedded {
				m[embName] = embValue
			}
		} else if option == "string" {
			kind := fv.Kind()
			if kind == reflect.Int || kind == reflect.Int8 || kind == reflect.Int16 || kind == reflect.Int32 || kind == reflect.Int64 {
				m[name] = strconv.FormatInt(fv.Int(), 10)
			} else if kind == reflect.Uint || kind == reflect.Uint8 || kind == reflect.Uint16 || kind == reflect.Uint32 || kind == reflect.Uint64 {
				m[name] = strconv.FormatUint(fv.Uint(), 10)
			} else if kind == reflect.Float32 || kind == reflect.Float64 {
				m[name] = strconv.FormatFloat(fv.Float(), 'f', 2, 64)
			} else {
				m[name] = fv.Interface()
			}
		} else {
			m[name] = fv.Interface()
		}
	}
	return m
}

func isEmpty(v *reflect.Value) bool {
	k := v.Kind()
	if k == reflect.Bool {
		return v.Bool() == false
	} else if reflect.Int < k && k < reflect.Int64 {
		return v.Int() == 0
	} else if reflect.Uint < k && k < reflect.Uintptr {
		return v.Uint() == 0
	} else if k == reflect.Float32 || k == reflect.Float64 {
		return v.Float() == 0
	} else if k == reflect.Array || k == reflect.Map || k == reflect.Slice || k == reflect.String {
		return v.Len() == 0
	} else if k == reflect.Interface || k == reflect.Ptr {
		return v.IsNil()
	}
	return false
}

func Decimal(num float64) float64 {
	num, _ = strconv.ParseFloat(fmt.Sprintf("%.3f", num), 64)
	return num
}

func foundKv(kv []*KV, key string) bool {
	for _, k := range kv {
		if k.Key == key {
			return true
		}
	}
	return false
}

func modifyKv(kv []*KV, key string, value int64) {
	for _, k := range kv {
		if k.Key == key {
			k.Value += value
			break
		}
	}
}

func addKv(kv []*KV, key string, value int64) []*KV {
	kvs := append(kv, &KV{
		Key:   key,
		Value: value,
	})
	return kvs
}
