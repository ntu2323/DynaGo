package outbox

import (
	"strings"
	"testing"

	"dynago/internal/types"
	"dynago/internal/wire"
)

func ack(req uint64) *wire.ReplicatePrepareReply {
	return &wire.ReplicatePrepareReply{ClientID: 7, RequestID: req, ViewID: 0, ServerID: 1}
}

func TestBgOutbox_FullByEntryCountAndTakeEmpties(t *testing.T) {
	box := New(3, 3, 0)
	if box.AddPrepareReply(1, ack(1)) || box.AddPrepareReply(1, ack(2)) {
		t.Fatal("not full yet")
	}
	if box.AddPrepareReply(2, ack(3)) {
		t.Fatal("other destination has its own count")
	}
	if !box.AddPrepareReply(1, ack(4)) {
		t.Fatal("3rd entry for r1 -> full")
	}
	b, n, ok := box.Take(1)
	if !ok || n != 3 || len(b.PrepareReplies) != 3 {
		t.Fatal(n, ok)
	}
	if b.PrepareReplies[0].RequestID != 1 || b.PrepareReplies[2].RequestID != 4 {
		t.Fatal("order")
	}
	if box.Pending(1) != 0 {
		t.Fatal()
	}
	if _, _, ok := box.Take(1); ok {
		t.Fatal("now empty")
	}
	if box.Pending(2) != 1 {
		t.Fatal()
	}
}

func TestBgOutbox_FullByBytesAndTakeAllMixesKinds(t *testing.T) {
	box := New(3, 1000, 200)
	c := &wire.ReplicateCommit{ClientID: 1, RequestID: 1}
	if box.AddCommit(2, c) || box.AddCommit(0, c) || box.AddCommit(9, c) {
		t.Fatal("small / out of range: not full")
	}
	p := &wire.ReplicatePrepare{ClientID: 1, RequestID: 2, AgreedT: 100,
		Txn: &types.Txn{WSet: []types.KV{{Key: strings.Repeat("k", 24), Value: strings.Repeat("v", 300)}}}}
	if !box.AddPrepare(2, p) {
		t.Fatal("the big body crosses 200 bytes")
	}
	all := box.TakeAll()
	if len(all) != 2 || all[0].To != 0 || len(all[0].Batch.Commits) != 1 {
		t.Fatal(all)
	}
	if all[1].To != 2 || len(all[1].Batch.Commits) != 1 || len(all[1].Batch.Prepares) != 1 {
		t.Fatal("kinds share one batch")
	}
	if all[1].Batch.Prepares[0].Txn.WSet[0].Value != strings.Repeat("v", 300) {
		t.Fatal()
	}
	if len(box.TakeAll()) != 0 {
		t.Fatal()
	}
}
