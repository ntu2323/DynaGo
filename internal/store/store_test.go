package store

import (
	"math"
	"strings"
	"testing"
)

func TestValueCodec_RoundTripsValueAndTimestamp(t *testing.T) {
	for _, ts := range []uint64{0, 1, 1791838758056582, math.MaxUint64} {
		for _, v := range []string{"", "x", strings.Repeat("z", 100), "a\x00b"} {
			enc := EncodeVersioned(nil, v, ts)
			if len(enc) != ValueHeaderBytes+len(v) {
				t.Fatal(len(enc))
			}
			dec := DecodeVersioned(enc)
			if dec.T != ts || dec.Value != v {
				t.Fatal(dec, ts, v)
			}
		}
	}
}

func TestValueCodec_ShortForeignBlobDecodesAsUnversioned(t *testing.T) {
	dec := DecodeVersioned([]byte("abc"))
	if dec.T != 0 || dec.Value != "abc" {
		t.Fatal(dec)
	}
}

func TestMem_AppliesAndReadsVersionedValues(t *testing.T) {
	m := NewMem()
	if _, ok := m.Read("k"); ok {
		t.Fatal()
	}
	m.Apply("k", "v1", 100)
	if v, ok := m.Read("k"); !ok || v.Value != "v1" || v.T != 100 {
		t.Fatal(v)
	}
	m.Apply("k", "v2", 200)
	if v, _ := m.Read("k"); v.Value != "v2" || v.T != 200 {
		t.Fatal(v)
	}
	if m.ApplyCount() != 2 {
		t.Fatal()
	}
}
