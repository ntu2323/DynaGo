package quorum

import (
	"testing"

	"dynago/internal/types"
)

func set(ids ...uint32) types.ServerSet {
	var s types.ServerSet
	for _, i := range ids {
		s.Add(i)
	}
	return s
}

func TestTxnQuorum_SinglePartitionFastPath(t *testing.T) {
	q := New(set(0), 2, 0)
	q.AddReply(0, 0, 100, true)
	if q.FastReady() {
		t.Fatal("leader alone < quorum")
	}
	q.AddReply(1, 0, 100, false)
	if !q.FastReady() || q.AgreedT() != 100 {
		t.Fatal()
	}
}

func TestTxnQuorum_MultiPartitionFastPathAllLeaders(t *testing.T) {
	q := New(set(0, 1), 2, 0)
	q.AddReply(0, 0, 200, true)
	if q.FastReady() || q.AllLeadersPresent() {
		t.Fatal()
	}
	q.AddReply(1, 0, 200, true)
	if !q.AllLeadersPresent() || !q.FastReady() || q.AgreedT() != 200 {
		t.Fatal()
	}
}

func TestTxnQuorum_MissingLeaderBlocksCommit(t *testing.T) {
	q := New(set(0, 1), 2, 0)
	q.AddReply(0, 0, 200, true)
	q.AddReply(2, 0, 200, false)
	q.AddReply(3, 0, 200, false)
	if q.AllLeadersPresent() || q.FastReady() || q.LeadersDisagree() {
		t.Fatal()
	}
}

func TestTxnQuorum_LeaderDisagreementTriggersSlowPath(t *testing.T) {
	q := New(set(0, 1), 2, 0)
	q.AddReply(0, 0, 100, true)
	q.AddReply(1, 0, 150, true)
	if !q.AllLeadersPresent() || q.FastReady() || !q.LeadersDisagree() || q.SlowAgreedT() != 150 {
		t.Fatal()
	}
}

func TestTxnQuorum_SlowPathAdoptionReformsQuorumAtAgreedT(t *testing.T) {
	q := New(set(0, 1), 2, 0)
	q.AddReply(0, 0, 100, true)
	q.AddReply(1, 0, 150, true)
	agreed := q.SlowAgreedT()
	if !q.LeadersDisagree() || agreed != 150 {
		t.Fatal()
	}
	q.AddReply(0, 0, agreed, true)
	q.AddReply(1, 0, agreed, true)
	if !q.FastReady() || q.AgreedT() != 150 {
		t.Fatal()
	}
}

func TestTxnQuorum_StaleFollowerReplyStopsMatching(t *testing.T) {
	q := New(set(0, 1), 3, 0)
	q.AddReply(0, 0, 150, true)
	q.AddReply(1, 0, 150, true)
	q.AddReply(2, 0, 100, false)
	if q.FastReady() {
		t.Fatal("only 2 at t=150")
	}
	q.AddReply(2, 0, 150, false)
	if !q.FastReady() {
		t.Fatal()
	}
}

func TestTxnQuorum_RepliesFromAnotherViewIgnored(t *testing.T) {
	q := New(set(0), 2, 1)
	if q.AddReply(0, 2, 100, false) || q.AddReply(1, 2, 100, false) || q.FastReady() || q.ReplyCount() != 0 {
		t.Fatal()
	}
	if !q.AddReply(0, 1, 100, true) || !q.AddReply(1, 1, 100, false) || !q.FastReady() {
		t.Fatal()
	}
}
