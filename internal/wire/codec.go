// Package wire is DYNA's Go-native message format: a hand-rolled binary codec
// (one type byte, then fields as uvarints / length-prefixed bytes). It is NOT
// wire-compatible with the C++ protobuf messages (user decision). The message set
// mirrors dyna-proto.proto one to one.
package wire

import (
	"encoding/binary"
	"errors"

	"dynago/internal/types"
)

var ErrShort = errors.New("wire: truncated message")
var ErrType = errors.New("wire: unknown message type")

// Writer appends fields to a byte slice.
type Writer struct{ B []byte }

func (w *Writer) U64(v uint64) { w.B = binary.AppendUvarint(w.B, v) }
func (w *Writer) U32(v uint32) { w.B = binary.AppendUvarint(w.B, uint64(v)) }
func (w *Writer) Bool(v bool) {
	if v {
		w.B = append(w.B, 1)
	} else {
		w.B = append(w.B, 0)
	}
}
func (w *Writer) Str(s string)   { w.U64(uint64(len(s))); w.B = append(w.B, s...) }
func (w *Writer) Bytes(b []byte) { w.U64(uint64(len(b))); w.B = append(w.B, b...) }

// Reader decodes fields; the first error sticks and later reads return zero values.
type Reader struct {
	B   []byte
	Err error
}

func (r *Reader) U64() uint64 {
	if r.Err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.B)
	if n <= 0 {
		r.Err = ErrShort
		return 0
	}
	r.B = r.B[n:]
	return v
}
func (r *Reader) U32() uint32 { return uint32(r.U64()) }
func (r *Reader) Bool() bool {
	if r.Err != nil {
		return false
	}
	if len(r.B) < 1 {
		r.Err = ErrShort
		return false
	}
	v := r.B[0] != 0
	r.B = r.B[1:]
	return v
}
func (r *Reader) raw() []byte {
	n := r.U64()
	if r.Err != nil {
		return nil
	}
	if uint64(len(r.B)) < n {
		r.Err = ErrShort
		return nil
	}
	b := r.B[:n]
	r.B = r.B[n:]
	return b
}

// Str copies (strings must not alias a pooled receive buffer).
func (r *Reader) Str() string { return string(r.raw()) }

// Bytes copies.
func (r *Reader) Bytes() []byte { return append([]byte(nil), r.raw()...) }

// Len reads a repeated-field count, bounded by the remaining bytes (each element is
// at least one byte) so a corrupt count can't trigger a huge allocation.
func (r *Reader) Len() int {
	n := r.U64()
	if n > uint64(len(r.B)) {
		r.Err = ErrShort
		return 0
	}
	return int(n)
}

// --- shared sub-structures ------------------------------------------------------

func putTxn(w *Writer, t *types.Txn) {
	w.U64(t.ClientID)
	w.U64(t.RequestID)
	w.B = append(w.B, byte(t.Type))
	w.U64(uint64(len(t.RSet)))
	for _, k := range t.RSet {
		w.Str(k)
	}
	w.U64(uint64(len(t.WSet)))
	for _, kv := range t.WSet {
		w.Str(kv.Key)
		w.Str(kv.Value)
	}
	w.U64(t.T)
	w.U64(t.ViewID)
	w.U64(t.SendTime)
}

func getTxn(r *Reader, t *types.Txn) {
	t.ClientID = r.U64()
	t.RequestID = r.U64()
	t.Type = types.TxnType(r.U64())
	if n := r.Len(); n > 0 {
		t.RSet = make([]string, n)
		for i := range t.RSet {
			t.RSet[i] = r.Str()
		}
	}
	if n := r.Len(); n > 0 {
		t.WSet = make([]types.KV, n)
		for i := range t.WSet {
			t.WSet[i].Key = r.Str()
			t.WSet[i].Value = r.Str()
		}
	}
	t.T = r.U64()
	t.ViewID = r.U64()
	t.SendTime = r.U64()
}

func putKVs(w *Writer, kvs []types.KV) {
	w.U64(uint64(len(kvs)))
	for _, kv := range kvs {
		w.Str(kv.Key)
		w.Str(kv.Value)
	}
}

func getKVs(r *Reader) []types.KV {
	n := r.Len()
	if n == 0 {
		return nil
	}
	out := make([]types.KV, n)
	for i := range out {
		out[i].Key = r.Str()
		out[i].Value = r.Str()
	}
	return out
}

// RecTxn: a txn as carried in recovery state (M) and snapshots.
type RecTxn struct {
	Txn    types.Txn
	T      uint64
	Status types.Status
}

// RecKV: an executed key-value with the t of the write that produced it.
type RecKV struct {
	Key   string
	Value string
	T     uint64
}

func putRecTxns(w *Writer, ts []RecTxn) {
	w.U64(uint64(len(ts)))
	for i := range ts {
		putTxn(w, &ts[i].Txn)
		w.U64(ts[i].T)
		w.B = append(w.B, byte(ts[i].Status))
	}
}

func getRecTxns(r *Reader) []RecTxn {
	n := r.Len()
	if n == 0 {
		return nil
	}
	out := make([]RecTxn, n)
	for i := range out {
		getTxn(r, &out[i].Txn)
		out[i].T = r.U64()
		out[i].Status = types.Status(r.U64())
	}
	return out
}

func putRecKVs(w *Writer, kvs []RecKV) {
	w.U64(uint64(len(kvs)))
	for _, kv := range kvs {
		w.Str(kv.Key)
		w.Str(kv.Value)
		w.U64(kv.T)
	}
}

func getRecKVs(r *Reader) []RecKV {
	n := r.Len()
	if n == 0 {
		return nil
	}
	out := make([]RecKV, n)
	for i := range out {
		out[i].Key = r.Str()
		out[i].Value = r.Str()
		out[i].T = r.U64()
	}
	return out
}
