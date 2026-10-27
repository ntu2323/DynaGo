package wire

import (
	"reflect"
	"strings"
	"testing"

	"dynago/internal/types"
)

func roundTrip(t *testing.T, m Message) Message {
	t.Helper()
	b := Marshal(nil, m)
	out, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("%T: %v", m, err)
	}
	if !reflect.DeepEqual(m, out) {
		t.Fatalf("%T mismatch:\n in  %+v\n out %+v", m, m, out)
	}
	// Every strict prefix must fail cleanly (no panic, no bogus success).
	for i := 0; i < len(b); i++ {
		if _, err := Unmarshal(b[:i]); err == nil && i < len(b) {
			// a prefix may decode only if the remaining fields are all zero-length;
			// the codec reads every field, so a prefix always runs out.
			t.Fatalf("%T: prefix %d/%d decoded", m, i, len(b))
		}
	}
	return out
}

func sampleTxn() types.Txn {
	return types.Txn{ClientID: 7, RequestID: 42, Type: types.Write, RSet: []string{"rk1"},
		WSet: []types.KV{{Key: "wk1", Value: "v1"}}, T: 1500, ViewID: 3, SendTime: 1200}
}

func TestSerialization_ClientRequestRoundTrip(t *testing.T) {
	roundTrip(t, &ClientRequest{Txn: sampleTxn()})
}

func TestSerialization_SpecReplyRoundTrip(t *testing.T) {
	roundTrip(t, &SpecReply{ClientID: 7, RequestID: 42, ViewID: 1, ServerID: 2, T: 1600,
		IsLeader: true, Results: []types.KV{{Key: "k", Value: "readval"}}, OwdEstimate: 250})
}

func TestSerialization_FastAndSlowCommitRoundTrip(t *testing.T) {
	roundTrip(t, &FastCommit{ClientID: 7, RequestID: 42, ViewID: 1, AgreedT: 1700})
	roundTrip(t, &SlowCommit{ClientID: 7, RequestID: 42, ViewID: 1, AgreedT: 1800})
}

func TestSerialization_ReplicatePrepareOptionalBody(t *testing.T) {
	p := &ReplicatePrepare{ClientID: 7, RequestID: 42, ViewID: 1, LeaderID: 0, AgreedT: 1900}
	if out := roundTrip(t, p).(*ReplicatePrepare); out.Txn != nil || out.AgreedT != 1900 {
		t.Fatal("body must stay absent")
	}
	body := sampleTxn()
	p.Txn = &body
	if out := roundTrip(t, p).(*ReplicatePrepare); out.Txn == nil || out.Txn.ClientID != 7 {
		t.Fatal("body lost")
	}
}

// Every message type round-trips with every field populated.
func TestSerialization_AllMessagesRoundTrip(t *testing.T) {
	txn := sampleTxn()
	rt := []RecTxn{{Txn: txn, T: 9, Status: types.Committed}}
	rk := []RecKV{{Key: "a", Value: "b", T: 5}}
	msgs := []Message{
		&ClientRequest{Txn: txn},
		&SpecReply{1, 2, 3, 4, 5, true, []types.KV{{Key: "k", Value: "v"}}, 6},
		&RejectReply{1, 2, 3, 4},
		&FastCommit{1, 2, 3, 4},
		&SlowCommit{1, 2, 3, 4},
		&FastCommitBatch{Commits: []FastCommit{{1, 2, 3, 4}, {5, 6, 7, 8}}},
		&SpecReplyBatch{Replies: []SpecReply{{ClientID: 1, RequestID: 2, T: 9}}},
		&CommitReply{1, 2, 3, 4, 5, true},
		&FetchRequest{1, 2, 3, 4},
		&FetchReply{Txn: txn},
		&TsAgree{1, 2, 3, 4, 5, true},
		&ReplicatePrepare{1, 2, 3, 4, 5, &txn},
		&ReplicatePrepareReply{1, 2, 3, 4},
		&ReplicateCommit{1, 2, 3, 4},
		&BgBatch{SenderID: 2, Prepares: []ReplicatePrepare{{1, 2, 3, 4, 5, nil}},
			PrepareReplies: []ReplicatePrepareReply{{1, 2, 3, 4}},
			Commits:        []ReplicateCommit{{1, 2, 3, 4}}, TsAgrees: []TsAgree{{1, 2, 3, 4, 5, false}}},
		&Heartbeat{1, 2},
		&StartViewChange{1, 2, 3},
		&DoViewChange{1, 2, 3, 4, true, 5},
		&StartRecovery{1, 2, 3, 4},
		&RecoveryRequest{1, 2, 3, 4},
		&RecoveryReply{1, 2, rt, rk},
		&RecoveryPrepare{1, 2, 3, 4, rt, rk},
		&RecoveryPrepareReply{1, 2, 3},
		&RecoveryReport{1, 2},
		&StartView{1, 2, 3},
		&ViewInfo{1, 2},
		&JoinRequest{1},
		&JoinInfo{1, 2, 3},
		&SnapshotRequest{1, 2, 3, 4},
		&SnapshotChunk{1, 2, 3, 4, 5, true, rk, rt, 6},
		&JoinReady{1, 2},
		&BulkChunk{1, 2, TRecoveryPrepare, 3, 4, []byte(strings.Repeat("x", 300))},
		&BulkAck{1, 2, true},
		&BulkPoke{1, 2},
	}
	seen := map[MsgType]bool{}
	for _, m := range msgs {
		roundTrip(t, m)
		seen[m.Type()] = true
	}
	for ty := TClientRequest; ty < tEnd; ty++ {
		if !seen[ty] && ty != 0 {
			t.Errorf("message type %d has no round-trip case", ty)
		}
	}
}

func TestSerialization_RejectsGarbage(t *testing.T) {
	if _, err := Unmarshal(nil); err == nil {
		t.Fatal("empty")
	}
	if _, err := Unmarshal([]byte{255}); err != ErrType {
		t.Fatal("unknown type")
	}
	// A huge repeated count must not allocate: fails as truncated.
	b := []byte{byte(TFastCommitBatch), 0xff, 0xff, 0xff, 0xff, 0x0f}
	if _, err := Unmarshal(b); err == nil {
		t.Fatal("bogus count accepted")
	}
}
