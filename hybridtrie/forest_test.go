package hybridtrie

import (
	"bytes"
	"testing"
)

func TestStorageLifecycleEncodingAndFullIDHeatKeys(t *testing.T) {
	for _, life := range []StorageLifecycle{{}, {true, 1}, {true, 1<<32 + 1}, {true, MaxTrieID}} {
		raw, err := encodeStorageLifecycle(life)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeStorageLifecycle(raw)
		if err != nil || got != life {
			t.Fatal(got, err)
		}
		if life.Active && len(raw) != 7 {
			t.Fatal("ID not six bytes")
		}
	}
	for _, raw := range [][]byte{nil, {0, 0}, {1}, make([]byte, 7), {1, 0, 0, 0, 0, 0, 0}} {
		if _, err := decodeStorageLifecycle(raw); err == nil {
			t.Fatal("invalid lifecycle accepted", raw)
		}
	}
	for _, life := range []StorageLifecycle{{true, 0}, {true, MaxTrieID + 1}, {false, 1}} {
		if _, err := encodeStorageLifecycle(life); err == nil {
			t.Fatal("invalid storage ID accepted", life)
		}
	}
	for _, path := range [][]byte{nil, {1, 2, 3}} {
		if positionHash(1, path) == positionHash(1<<32+1, path) {
			t.Fatal("statistics discarded upper ID bits")
		}
	}
	c, err := newResidentCache(DefaultCacheConfig())
	if err != nil {
		t.Fatal(err)
	}
	c.trieID = 1<<40 + 7
	if err = c.observeBody([]byte{1}, 1); err != nil {
		t.Fatal(err)
	}
	if c.policy.heat.estimate(c.trieID, []byte{1}) == 0 || c.policy.heat.estimate(7, []byte{1}) != 0 {
		t.Fatal("CMS namespace truncation")
	}
	var address [20]byte
	address[19] = 1
	key := lifecycleKey(address, MaxHeight)
	if len(key) != 27 || !bytes.Equal(key[21:], bytes.Repeat([]byte{0xff}, 6)) {
		t.Fatal("lifecycle height width")
	}
}
