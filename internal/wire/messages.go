package wire

import "dynago/internal/types"

type MsgType uint8

const (
	TClientRequest MsgType = iota + 1
	TSpecReply
	TRejectReply
	TFastCommit
	TSlowCommit
	TFastCommitBatch
	TSpecReplyBatch
	TCommitReply
	TFetchRequest
	TFetchReply
	TTsAgree
	TReplicatePrepare
	TReplicatePrepareReply
	TReplicateCommit
	TBgBatch
	THeartbeat
	TStartViewChange
	TDoViewChange
	TStartRecovery
	TRecoveryRequest
	TRecoveryReply
	TRecoveryPrepare
	TRecoveryPrepareReply
	TRecoveryReport
	TStartView
	TViewInfo
	TJoinRequest
	TJoinInfo
	TSnapshotRequest
	TSnapshotChunk
	TJoinReady
	TBulkChunk
	TBulkAck
	TBulkPoke
	tEnd
)

// Message is every wire message.
type Message interface {
	Type() MsgType
	encode(w *Writer)
	decode(r *Reader)
}

// Marshal appends [type][body] to dst. The concrete type switch (rather than an
// interface call) keeps the Writer on the stack.
func Marshal(dst []byte, m Message) []byte {
	w := Writer{B: dst}
	switch m := m.(type) {
	case *ClientRequest:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *SpecReply:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *RejectReply:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *FastCommit:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *SlowCommit:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *FastCommitBatch:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *SpecReplyBatch:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *CommitReply:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *FetchRequest:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *FetchReply:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *TsAgree:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *ReplicatePrepare:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *ReplicatePrepareReply:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *ReplicateCommit:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *BgBatch:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *Heartbeat:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *StartViewChange:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *DoViewChange:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *StartRecovery:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *RecoveryRequest:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *RecoveryReply:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *RecoveryPrepare:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *RecoveryPrepareReply:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *RecoveryReport:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *StartView:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *ViewInfo:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *JoinRequest:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *JoinInfo:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *SnapshotRequest:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *SnapshotChunk:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *JoinReady:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *BulkChunk:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *BulkAck:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	case *BulkPoke:
		w.B = append(w.B, byte(m.Type()))
		m.encode(&w)
	default:
		panic("wire: Marshal of an unregistered message type")
	}
	return w.B
}

// New returns a zero message of type t (nil if unknown).
func New(t MsgType) Message {
	switch t {
	case TClientRequest:
		return &ClientRequest{}
	case TSpecReply:
		return &SpecReply{}
	case TRejectReply:
		return &RejectReply{}
	case TFastCommit:
		return &FastCommit{}
	case TSlowCommit:
		return &SlowCommit{}
	case TFastCommitBatch:
		return &FastCommitBatch{}
	case TSpecReplyBatch:
		return &SpecReplyBatch{}
	case TCommitReply:
		return &CommitReply{}
	case TFetchRequest:
		return &FetchRequest{}
	case TFetchReply:
		return &FetchReply{}
	case TTsAgree:
		return &TsAgree{}
	case TReplicatePrepare:
		return &ReplicatePrepare{}
	case TReplicatePrepareReply:
		return &ReplicatePrepareReply{}
	case TReplicateCommit:
		return &ReplicateCommit{}
	case TBgBatch:
		return &BgBatch{}
	case THeartbeat:
		return &Heartbeat{}
	case TStartViewChange:
		return &StartViewChange{}
	case TDoViewChange:
		return &DoViewChange{}
	case TStartRecovery:
		return &StartRecovery{}
	case TRecoveryRequest:
		return &RecoveryRequest{}
	case TRecoveryReply:
		return &RecoveryReply{}
	case TRecoveryPrepare:
		return &RecoveryPrepare{}
	case TRecoveryPrepareReply:
		return &RecoveryPrepareReply{}
	case TRecoveryReport:
		return &RecoveryReport{}
	case TStartView:
		return &StartView{}
	case TViewInfo:
		return &ViewInfo{}
	case TJoinRequest:
		return &JoinRequest{}
	case TJoinInfo:
		return &JoinInfo{}
	case TSnapshotRequest:
		return &SnapshotRequest{}
	case TSnapshotChunk:
		return &SnapshotChunk{}
	case TJoinReady:
		return &JoinReady{}
	case TBulkChunk:
		return &BulkChunk{}
	case TBulkAck:
		return &BulkAck{}
	case TBulkPoke:
		return &BulkPoke{}
	}
	return nil
}

// Unmarshal decodes one [type][body] message. Strings are copied, so b may be reused.
func Unmarshal(b []byte) (Message, error) {
	if len(b) == 0 {
		return nil, ErrShort
	}
	// A concrete type switch (not New + an interface call) keeps the Reader on the stack.
	r := Reader{B: b[1:]}
	var m Message
	switch MsgType(b[0]) {
	case TClientRequest:
		x := &ClientRequest{}
		x.decode(&r)
		m = x
	case TSpecReply:
		x := &SpecReply{}
		x.decode(&r)
		m = x
	case TRejectReply:
		x := &RejectReply{}
		x.decode(&r)
		m = x
	case TFastCommit:
		x := &FastCommit{}
		x.decode(&r)
		m = x
	case TSlowCommit:
		x := &SlowCommit{}
		x.decode(&r)
		m = x
	case TFastCommitBatch:
		x := &FastCommitBatch{}
		x.decode(&r)
		m = x
	case TSpecReplyBatch:
		x := &SpecReplyBatch{}
		x.decode(&r)
		m = x
	case TCommitReply:
		x := &CommitReply{}
		x.decode(&r)
		m = x
	case TFetchRequest:
		x := &FetchRequest{}
		x.decode(&r)
		m = x
	case TFetchReply:
		x := &FetchReply{}
		x.decode(&r)
		m = x
	case TTsAgree:
		x := &TsAgree{}
		x.decode(&r)
		m = x
	case TReplicatePrepare:
		x := &ReplicatePrepare{}
		x.decode(&r)
		m = x
	case TReplicatePrepareReply:
		x := &ReplicatePrepareReply{}
		x.decode(&r)
		m = x
	case TReplicateCommit:
		x := &ReplicateCommit{}
		x.decode(&r)
		m = x
	case TBgBatch:
		x := &BgBatch{}
		x.decode(&r)
		m = x
	case THeartbeat:
		x := &Heartbeat{}
		x.decode(&r)
		m = x
	case TStartViewChange:
		x := &StartViewChange{}
		x.decode(&r)
		m = x
	case TDoViewChange:
		x := &DoViewChange{}
		x.decode(&r)
		m = x
	case TStartRecovery:
		x := &StartRecovery{}
		x.decode(&r)
		m = x
	case TRecoveryRequest:
		x := &RecoveryRequest{}
		x.decode(&r)
		m = x
	case TRecoveryReply:
		x := &RecoveryReply{}
		x.decode(&r)
		m = x
	case TRecoveryPrepare:
		x := &RecoveryPrepare{}
		x.decode(&r)
		m = x
	case TRecoveryPrepareReply:
		x := &RecoveryPrepareReply{}
		x.decode(&r)
		m = x
	case TRecoveryReport:
		x := &RecoveryReport{}
		x.decode(&r)
		m = x
	case TStartView:
		x := &StartView{}
		x.decode(&r)
		m = x
	case TViewInfo:
		x := &ViewInfo{}
		x.decode(&r)
		m = x
	case TJoinRequest:
		x := &JoinRequest{}
		x.decode(&r)
		m = x
	case TJoinInfo:
		x := &JoinInfo{}
		x.decode(&r)
		m = x
	case TSnapshotRequest:
		x := &SnapshotRequest{}
		x.decode(&r)
		m = x
	case TSnapshotChunk:
		x := &SnapshotChunk{}
		x.decode(&r)
		m = x
	case TJoinReady:
		x := &JoinReady{}
		x.decode(&r)
		m = x
	case TBulkChunk:
		x := &BulkChunk{}
		x.decode(&r)
		m = x
	case TBulkAck:
		x := &BulkAck{}
		x.decode(&r)
		m = x
	case TBulkPoke:
		x := &BulkPoke{}
		x.decode(&r)
		m = x
	default:
		return nil, ErrType
	}
	if r.Err != nil {
		return nil, r.Err
	}
	return m, nil
}

// PeekType returns the type byte of an encoded message (0 if empty).
func PeekType(b []byte) MsgType {
	if len(b) == 0 {
		return 0
	}
	return MsgType(b[0])
}

// --- client path -----------------------------------------------------------------

type ClientRequest struct{ Txn types.Txn }

func (*ClientRequest) Type() MsgType      { return TClientRequest }
func (m *ClientRequest) encode(w *Writer) { putTxn(w, &m.Txn) }
func (m *ClientRequest) decode(r *Reader) { getTxn(r, &m.Txn) }

type SpecReply struct {
	ClientID, RequestID, ViewID uint64
	ServerID                    uint32
	T                           uint64 // the LIVE t this server released at
	IsLeader                    bool   // this server leads a key in the txn
	Results                     []types.KV
	OwdEstimate                 uint64 // µs, piggybacked OWD feedback
}

func (*SpecReply) Type() MsgType { return TSpecReply }
func (m *SpecReply) encode(w *Writer) {
	w.U64(m.ClientID)
	w.U64(m.RequestID)
	w.U64(m.ViewID)
	w.U32(m.ServerID)
	w.U64(m.T)
	w.Bool(m.IsLeader)
	putKVs(w, m.Results)
	w.U64(m.OwdEstimate)
}
func (m *SpecReply) decode(r *Reader) {
	m.ClientID, m.RequestID, m.ViewID = r.U64(), r.U64(), r.U64()
	m.ServerID = r.U32()
	m.T = r.U64()
	m.IsLeader = r.Bool()
	m.Results = getKVs(r)
	m.OwdEstimate = r.U64()
}
func (m *SpecReply) Id() types.TxnId { return types.ID(m.ClientID, m.RequestID) }

type RejectReply struct {
	ClientID, RequestID, ViewID uint64
	ServerID                    uint32
}

func (*RejectReply) Type() MsgType { return TRejectReply }
func (m *RejectReply) encode(w *Writer) {
	w.U64(m.ClientID)
	w.U64(m.RequestID)
	w.U64(m.ViewID)
	w.U32(m.ServerID)
}
func (m *RejectReply) decode(r *Reader) {
	m.ClientID, m.RequestID, m.ViewID = r.U64(), r.U64(), r.U64()
	m.ServerID = r.U32()
}

// FastCommit: all leaders matched; AgreedT is the common t.
type FastCommit struct{ ClientID, RequestID, ViewID, AgreedT uint64 }

func (*FastCommit) Type() MsgType { return TFastCommit }
func (m *FastCommit) encode(w *Writer) {
	w.U64(m.ClientID)
	w.U64(m.RequestID)
	w.U64(m.ViewID)
	w.U64(m.AgreedT)
}
func (m *FastCommit) decode(r *Reader) {
	m.ClientID, m.RequestID, m.ViewID, m.AgreedT = r.U64(), r.U64(), r.U64(), r.U64()
}
func (m *FastCommit) Id() types.TxnId { return types.ID(m.ClientID, m.RequestID) }

// SlowCommit: AgreedT = max over the involved leaders' proposals.
type SlowCommit struct{ ClientID, RequestID, ViewID, AgreedT uint64 }

func (*SlowCommit) Type() MsgType { return TSlowCommit }
func (m *SlowCommit) encode(w *Writer) {
	w.U64(m.ClientID)
	w.U64(m.RequestID)
	w.U64(m.ViewID)
	w.U64(m.AgreedT)
}
func (m *SlowCommit) decode(r *Reader) {
	m.ClientID, m.RequestID, m.ViewID, m.AgreedT = r.U64(), r.U64(), r.U64(), r.U64()
}
func (m *SlowCommit) Id() types.TxnId { return types.ID(m.ClientID, m.RequestID) }

type FastCommitBatch struct{ Commits []FastCommit }

func (*FastCommitBatch) Type() MsgType { return TFastCommitBatch }
func (m *FastCommitBatch) encode(w *Writer) {
	w.U64(uint64(len(m.Commits)))
	for i := range m.Commits {
		m.Commits[i].encode(w)
	}
}
func (m *FastCommitBatch) decode(r *Reader) {
	m.Commits = make([]FastCommit, r.Len())
	for i := range m.Commits {
		m.Commits[i].decode(r)
	}
}

type SpecReplyBatch struct{ Replies []SpecReply }

func (*SpecReplyBatch) Type() MsgType { return TSpecReplyBatch }
func (m *SpecReplyBatch) encode(w *Writer) {
	w.U64(uint64(len(m.Replies)))
	for i := range m.Replies {
		m.Replies[i].encode(w)
	}
}
func (m *SpecReplyBatch) decode(r *Reader) {
	m.Replies = make([]SpecReply, r.Len())
	for i := range m.Replies {
		m.Replies[i].decode(r)
	}
}

type CommitReply struct {
	ClientID, RequestID, ViewID uint64
	ServerID                    uint32
	T                           uint64 // == agreed t after the server adopted it
	IsLeader                    bool
}

func (*CommitReply) Type() MsgType { return TCommitReply }
func (m *CommitReply) encode(w *Writer) {
	w.U64(m.ClientID)
	w.U64(m.RequestID)
	w.U64(m.ViewID)
	w.U32(m.ServerID)
	w.U64(m.T)
	w.Bool(m.IsLeader)
}
func (m *CommitReply) decode(r *Reader) {
	m.ClientID, m.RequestID, m.ViewID = r.U64(), r.U64(), r.U64()
	m.ServerID = r.U32()
	m.T = r.U64()
	m.IsLeader = r.Bool()
}
func (m *CommitReply) Id() types.TxnId { return types.ID(m.ClientID, m.RequestID) }

type FetchRequest struct {
	ClientID, RequestID, ViewID uint64
	RequesterID                 uint32
}

func (*FetchRequest) Type() MsgType { return TFetchRequest }
func (m *FetchRequest) encode(w *Writer) {
	w.U64(m.ClientID)
	w.U64(m.RequestID)
	w.U64(m.ViewID)
	w.U32(m.RequesterID)
}
func (m *FetchRequest) decode(r *Reader) {
	m.ClientID, m.RequestID, m.ViewID = r.U64(), r.U64(), r.U64()
	m.RequesterID = r.U32()
}

type FetchReply struct{ Txn types.Txn }

func (*FetchReply) Type() MsgType      { return TFetchReply }
func (m *FetchReply) encode(w *Writer) { putTxn(w, &m.Txn) }
func (m *FetchReply) decode(r *Reader) { getTxn(r, &m.Txn) }

// --- background ordering / replication -------------------------------------------

type TsAgree struct {
	ClientID, RequestID, ViewID uint64
	SenderID                    uint32
	ProposedT                   uint64 // this leader's current live t
	Reply                       bool   // answers a peer's TsAgree (never itself answered)
}

func (*TsAgree) Type() MsgType { return TTsAgree }
func (m *TsAgree) encode(w *Writer) {
	w.U64(m.ClientID)
	w.U64(m.RequestID)
	w.U64(m.ViewID)
	w.U32(m.SenderID)
	w.U64(m.ProposedT)
	w.Bool(m.Reply)
}
func (m *TsAgree) decode(r *Reader) {
	m.ClientID, m.RequestID, m.ViewID = r.U64(), r.U64(), r.U64()
	m.SenderID = r.U32()
	m.ProposedT = r.U64()
	m.Reply = r.Bool()
}
func (m *TsAgree) Id() types.TxnId { return types.ID(m.ClientID, m.RequestID) }

type ReplicatePrepare struct {
	ClientID, RequestID, ViewID uint64
	LeaderID                    uint32
	AgreedT                     uint64
	Txn                         *types.Txn // optional body, so a lacking follower can materialize
}

func (*ReplicatePrepare) Type() MsgType { return TReplicatePrepare }
func (m *ReplicatePrepare) encode(w *Writer) {
	w.U64(m.ClientID)
	w.U64(m.RequestID)
	w.U64(m.ViewID)
	w.U32(m.LeaderID)
	w.U64(m.AgreedT)
	w.Bool(m.Txn != nil)
	if m.Txn != nil {
		putTxn(w, m.Txn)
	}
}
func (m *ReplicatePrepare) decode(r *Reader) {
	m.ClientID, m.RequestID, m.ViewID = r.U64(), r.U64(), r.U64()
	m.LeaderID = r.U32()
	m.AgreedT = r.U64()
	if r.Bool() {
		m.Txn = new(types.Txn)
		getTxn(r, m.Txn)
	}
}
func (m *ReplicatePrepare) Id() types.TxnId { return types.ID(m.ClientID, m.RequestID) }

type ReplicatePrepareReply struct {
	ClientID, RequestID, ViewID uint64
	ServerID                    uint32
}

func (*ReplicatePrepareReply) Type() MsgType { return TReplicatePrepareReply }
func (m *ReplicatePrepareReply) encode(w *Writer) {
	w.U64(m.ClientID)
	w.U64(m.RequestID)
	w.U64(m.ViewID)
	w.U32(m.ServerID)
}
func (m *ReplicatePrepareReply) decode(r *Reader) {
	m.ClientID, m.RequestID, m.ViewID = r.U64(), r.U64(), r.U64()
	m.ServerID = r.U32()
}
func (m *ReplicatePrepareReply) Id() types.TxnId { return types.ID(m.ClientID, m.RequestID) }

type ReplicateCommit struct {
	ClientID, RequestID, ViewID uint64
	LeaderID                    uint32
}

func (*ReplicateCommit) Type() MsgType { return TReplicateCommit }
func (m *ReplicateCommit) encode(w *Writer) {
	w.U64(m.ClientID)
	w.U64(m.RequestID)
	w.U64(m.ViewID)
	w.U32(m.LeaderID)
}
func (m *ReplicateCommit) decode(r *Reader) {
	m.ClientID, m.RequestID, m.ViewID = r.U64(), r.U64(), r.U64()
	m.LeaderID = r.U32()
}
func (m *ReplicateCommit) Id() types.TxnId { return types.ID(m.ClientID, m.RequestID) }

// BgBatch carries per-destination background messages, unchanged, as one datagram.
type BgBatch struct {
	SenderID       uint32
	Prepares       []ReplicatePrepare
	PrepareReplies []ReplicatePrepareReply
	Commits        []ReplicateCommit
	TsAgrees       []TsAgree
}

func (*BgBatch) Type() MsgType { return TBgBatch }
func (m *BgBatch) encode(w *Writer) {
	w.U32(m.SenderID)
	w.U64(uint64(len(m.Prepares)))
	for i := range m.Prepares {
		m.Prepares[i].encode(w)
	}
	w.U64(uint64(len(m.PrepareReplies)))
	for i := range m.PrepareReplies {
		m.PrepareReplies[i].encode(w)
	}
	w.U64(uint64(len(m.Commits)))
	for i := range m.Commits {
		m.Commits[i].encode(w)
	}
	w.U64(uint64(len(m.TsAgrees)))
	for i := range m.TsAgrees {
		m.TsAgrees[i].encode(w)
	}
}
func (m *BgBatch) decode(r *Reader) {
	m.SenderID = r.U32()
	if n := r.Len(); n > 0 {
		m.Prepares = make([]ReplicatePrepare, n)
		for i := range m.Prepares {
			m.Prepares[i].decode(r)
		}
	}
	if n := r.Len(); n > 0 {
		m.PrepareReplies = make([]ReplicatePrepareReply, n)
		for i := range m.PrepareReplies {
			m.PrepareReplies[i].decode(r)
		}
	}
	if n := r.Len(); n > 0 {
		m.Commits = make([]ReplicateCommit, n)
		for i := range m.Commits {
			m.Commits[i].decode(r)
		}
	}
	if n := r.Len(); n > 0 {
		m.TsAgrees = make([]TsAgree, n)
		for i := range m.TsAgrees {
			m.TsAgrees[i].decode(r)
		}
	}
}

// Entries is the total number of carried messages.
func (m *BgBatch) Entries() int {
	return len(m.Prepares) + len(m.PrepareReplies) + len(m.Commits) + len(m.TsAgrees)
}

// --- VR layer / recovery -----------------------------------------------------------

type Heartbeat struct {
	ViewID   uint64
	SenderID uint32
}

func (*Heartbeat) Type() MsgType      { return THeartbeat }
func (m *Heartbeat) encode(w *Writer) { w.U64(m.ViewID); w.U32(m.SenderID) }
func (m *Heartbeat) decode(r *Reader) { m.ViewID = r.U64(); m.SenderID = r.U32() }

type StartViewChange struct {
	ViewID   uint64
	SenderID uint32
	Alive    uint64
}

func (*StartViewChange) Type() MsgType { return TStartViewChange }
func (m *StartViewChange) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.SenderID)
	w.U64(m.Alive)
}
func (m *StartViewChange) decode(r *Reader) {
	m.ViewID = r.U64()
	m.SenderID = r.U32()
	m.Alive = r.U64()
}

type DoViewChange struct {
	ViewID            uint64
	SenderID          uint32
	Alive             uint64
	OldAlive          uint64 // sender's last NORMAL alive set
	HasLastNormalView bool
	LastNormalView    uint64 // the view whose alive set OldAlive is
}

func (*DoViewChange) Type() MsgType { return TDoViewChange }
func (m *DoViewChange) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.SenderID)
	w.U64(m.Alive)
	w.U64(m.OldAlive)
	w.Bool(m.HasLastNormalView)
	w.U64(m.LastNormalView)
}
func (m *DoViewChange) decode(r *Reader) {
	m.ViewID = r.U64()
	m.SenderID = r.U32()
	m.Alive = r.U64()
	m.OldAlive = r.U64()
	m.HasLastNormalView = r.Bool()
	m.LastNormalView = r.U64()
}

type StartRecovery struct {
	ViewID   uint64
	SenderID uint32
	Alive    uint64
	OldAlive uint64 // the view being replaced (defines the failed range)
}

func (*StartRecovery) Type() MsgType { return TStartRecovery }
func (m *StartRecovery) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.SenderID)
	w.U64(m.Alive)
	w.U64(m.OldAlive)
}
func (m *StartRecovery) decode(r *Reader) {
	m.ViewID = r.U64()
	m.SenderID = r.U32()
	m.Alive = r.U64()
	m.OldAlive = r.U64()
}

type RecoveryRequest struct {
	ViewID      uint64
	RequesterID uint32
	Alive       uint64
	OldAlive    uint64
}

func (*RecoveryRequest) Type() MsgType { return TRecoveryRequest }
func (m *RecoveryRequest) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.RequesterID)
	w.U64(m.Alive)
	w.U64(m.OldAlive)
}
func (m *RecoveryRequest) decode(r *Reader) {
	m.ViewID = r.U64()
	m.RequesterID = r.U32()
	m.Alive = r.U64()
	m.OldAlive = r.U64()
}

type RecoveryReply struct {
	ViewID   uint64
	SenderID uint32
	Txns     []RecTxn
	KVs      []RecKV
}

func (*RecoveryReply) Type() MsgType { return TRecoveryReply }
func (m *RecoveryReply) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.SenderID)
	putRecTxns(w, m.Txns)
	putRecKVs(w, m.KVs)
}
func (m *RecoveryReply) decode(r *Reader) {
	m.ViewID = r.U64()
	m.SenderID = r.U32()
	m.Txns = getRecTxns(r)
	m.KVs = getRecKVs(r)
}

type RecoveryPrepare struct {
	ViewID   uint64
	SenderID uint32
	Alive    uint64
	OldAlive uint64
	Txns     []RecTxn
	KVs      []RecKV
}

func (*RecoveryPrepare) Type() MsgType { return TRecoveryPrepare }
func (m *RecoveryPrepare) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.SenderID)
	w.U64(m.Alive)
	w.U64(m.OldAlive)
	putRecTxns(w, m.Txns)
	putRecKVs(w, m.KVs)
}
func (m *RecoveryPrepare) decode(r *Reader) {
	m.ViewID = r.U64()
	m.SenderID = r.U32()
	m.Alive = r.U64()
	m.OldAlive = r.U64()
	m.Txns = getRecTxns(r)
	m.KVs = getRecKVs(r)
}

type RecoveryPrepareReply struct {
	ViewID   uint64
	SenderID uint32
	PLID     uint32
}

func (*RecoveryPrepareReply) Type() MsgType { return TRecoveryPrepareReply }
func (m *RecoveryPrepareReply) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.SenderID)
	w.U32(m.PLID)
}
func (m *RecoveryPrepareReply) decode(r *Reader) {
	m.ViewID = r.U64()
	m.SenderID = r.U32()
	m.PLID = r.U32()
}

type RecoveryReport struct {
	ViewID   uint64
	SenderID uint32
}

func (*RecoveryReport) Type() MsgType      { return TRecoveryReport }
func (m *RecoveryReport) encode(w *Writer) { w.U64(m.ViewID); w.U32(m.SenderID) }
func (m *RecoveryReport) decode(r *Reader) { m.ViewID = r.U64(); m.SenderID = r.U32() }

type StartView struct {
	ViewID   uint64
	SenderID uint32
	Alive    uint64
}

func (*StartView) Type() MsgType { return TStartView }
func (m *StartView) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.SenderID)
	w.U64(m.Alive)
}
func (m *StartView) decode(r *Reader) {
	m.ViewID = r.U64()
	m.SenderID = r.U32()
	m.Alive = r.U64()
}

// ViewInfo tells a client on an old view about the current one.
type ViewInfo struct{ ViewID, Alive uint64 }

func (*ViewInfo) Type() MsgType      { return TViewInfo }
func (m *ViewInfo) encode(w *Writer) { w.U64(m.ViewID); w.U64(m.Alive) }
func (m *ViewInfo) decode(r *Reader) { m.ViewID = r.U64(); m.Alive = r.U64() }

// --- rejoin ----------------------------------------------------------------------

type JoinRequest struct{ SenderID uint32 }

func (*JoinRequest) Type() MsgType      { return TJoinRequest }
func (m *JoinRequest) encode(w *Writer) { w.U32(m.SenderID) }
func (m *JoinRequest) decode(r *Reader) { m.SenderID = r.U32() }

type JoinInfo struct {
	ViewID   uint64
	SenderID uint32
	Alive    uint64
}

func (*JoinInfo) Type() MsgType { return TJoinInfo }
func (m *JoinInfo) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.SenderID)
	w.U64(m.Alive)
}
func (m *JoinInfo) decode(r *Reader) {
	m.ViewID = r.U64()
	m.SenderID = r.U32()
	m.Alive = r.U64()
}

type SnapshotRequest struct {
	ViewID      uint64
	RequesterID uint32
	Session     uint64 // a new session restarts the key enumeration
	Offset      uint64 // index into the donor's sorted key list
}

func (*SnapshotRequest) Type() MsgType { return TSnapshotRequest }
func (m *SnapshotRequest) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.RequesterID)
	w.U64(m.Session)
	w.U64(m.Offset)
}
func (m *SnapshotRequest) decode(r *Reader) {
	m.ViewID = r.U64()
	m.RequesterID = r.U32()
	m.Session = r.U64()
	m.Offset = r.U64()
}

type SnapshotChunk struct {
	ViewID     uint64
	SenderID   uint32
	Session    uint64
	Offset     uint64
	Total      uint64
	Done       bool
	KVs        []RecKV
	Txns       []RecTxn
	NextOffset uint64 // where the joiner's next request starts
}

func (*SnapshotChunk) Type() MsgType { return TSnapshotChunk }
func (m *SnapshotChunk) encode(w *Writer) {
	w.U64(m.ViewID)
	w.U32(m.SenderID)
	w.U64(m.Session)
	w.U64(m.Offset)
	w.U64(m.Total)
	w.Bool(m.Done)
	putRecKVs(w, m.KVs)
	putRecTxns(w, m.Txns)
	w.U64(m.NextOffset)
}
func (m *SnapshotChunk) decode(r *Reader) {
	m.ViewID = r.U64()
	m.SenderID = r.U32()
	m.Session = r.U64()
	m.Offset = r.U64()
	m.Total = r.U64()
	m.Done = r.Bool()
	m.KVs = getRecKVs(r)
	m.Txns = getRecTxns(r)
	m.NextOffset = r.U64()
}

type JoinReady struct {
	ViewID   uint64
	SenderID uint32
}

func (*JoinReady) Type() MsgType      { return TJoinReady }
func (m *JoinReady) encode(w *Writer) { w.U64(m.ViewID); w.U32(m.SenderID) }
func (m *JoinReady) decode(r *Reader) { m.ViewID = r.U64(); m.SenderID = r.U32() }

// --- chunked bulk transfer for large VR/recovery messages ---------------------------

type BulkChunk struct {
	SenderID uint32
	XferID   uint64  // hash of (sender, type, bytes): equal resends merge
	Inner    MsgType // the inner message's type
	Idx      uint32
	Total    uint32
	Data     []byte
}

func (*BulkChunk) Type() MsgType { return TBulkChunk }
func (m *BulkChunk) encode(w *Writer) {
	w.U32(m.SenderID)
	w.U64(m.XferID)
	w.B = append(w.B, byte(m.Inner))
	w.U32(m.Idx)
	w.U32(m.Total)
	w.Bytes(m.Data)
}
func (m *BulkChunk) decode(r *Reader) {
	m.SenderID = r.U32()
	m.XferID = r.U64()
	m.Inner = MsgType(r.U64())
	m.Idx = r.U32()
	m.Total = r.U32()
	m.Data = r.Bytes()
}

type BulkAck struct {
	SenderID uint32
	XferID   uint64
	Have     bool
}

func (*BulkAck) Type() MsgType { return TBulkAck }
func (m *BulkAck) encode(w *Writer) {
	w.U32(m.SenderID)
	w.U64(m.XferID)
	w.Bool(m.Have)
}
func (m *BulkAck) decode(r *Reader) {
	m.SenderID = r.U32()
	m.XferID = r.U64()
	m.Have = r.Bool()
}

type BulkPoke struct {
	SenderID uint32
	XferID   uint64
}

func (*BulkPoke) Type() MsgType      { return TBulkPoke }
func (m *BulkPoke) encode(w *Writer) { w.U32(m.SenderID); w.U64(m.XferID) }
func (m *BulkPoke) decode(r *Reader) { m.SenderID = r.U32(); m.XferID = r.U64() }
