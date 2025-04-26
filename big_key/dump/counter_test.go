package dump

import "testing"

func TestGetPrefixes(t *testing.T) {
	data := getPrefixes("kim:chatmsg:chat:member:sendstat", ":;,_- ")
	t.Log(data)
}
