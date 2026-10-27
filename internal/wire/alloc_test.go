package wire

import "testing"

// Encoding allocates nothing beyond the destination buffer (hot reply / commit path).
func TestCodec_MarshalDoesNotAllocate(t *testing.T) {
	buf := make([]byte, 0, 4096)
	msgs := []Message{
		&SpecReply{ClientID: 1, RequestID: 2, ViewID: 3, ServerID: 1, T: 99, IsLeader: true, OwdEstimate: 7},
		&FastCommit{ClientID: 1, RequestID: 2, ViewID: 3, AgreedT: 4},
		&SpecReplyBatch{Replies: []SpecReply{{ClientID: 1}, {ClientID: 2}}},
	}
	for _, m := range msgs {
		if n := testing.AllocsPerRun(100, func() { buf = Marshal(buf[:0], m) }); n != 0 {
			t.Errorf("%T: %.1f allocs per Marshal", m, n)
		}
	}
	b := Marshal(nil, &FastCommit{ClientID: 1})
	if n := testing.AllocsPerRun(100, func() { _, _ = Unmarshal(b) }); n != 1 {
		t.Errorf("Unmarshal(FastCommit): %.1f allocs, want 1 (the message)", n)
	}
}
