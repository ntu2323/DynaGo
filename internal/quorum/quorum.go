// Package quorum is the client-side commit rule (C++ dyna/commit_quorum.h, DECISION A)
// for multiple involved partition leaders. A write commits on the FAST path when:
//  1. every involved partition leader has replied,
//  2. all involved leaders carry the SAME (viewId, t), and
//  3. at least f+1 replies (leaders + followers) carry that same (viewId, t).
//
// If all leaders replied but disagree → SLOW path: SlowCommit(max leader t). Servers
// adopt it and send CommitReply(agreedT), which feed the SAME TxnQuorum (latest reply
// per server wins), so FastReady() fires once the quorum re-forms at agreedT.
package quorum

import "dynago/internal/types"

type reply struct {
	server uint32
	view   uint64
	t      uint64
}

type TxnQuorum struct {
	leaders types.ServerSet
	quorum  int
	view    uint64
	replies []reply // latest per server; n is small (≤ 64), so a slice beats a map
}

// New: `view` is the client's view, whose topology produced `leaders`. Replies from any
// other view are ignored: there a server may no longer (or may now) lead these keys,
// so two new-view followers could fake a fast commit under the old leader set.
func New(leaders types.ServerSet, quorum int, view uint64) TxnQuorum {
	return TxnQuorum{leaders: leaders, quorum: quorum, view: view}
}

// AddReply records the latest reply from a server (SpecReply or CommitReply); false if
// it is from another view (ignored).
func (q *TxnQuorum) AddReply(server uint32, view, t uint64, isLeader bool) bool {
	if view != q.view {
		return false
	}
	if isLeader {
		q.leaders.Add(server) // defensive
	}
	for i := range q.replies {
		if q.replies[i].server == server {
			q.replies[i] = reply{server, view, t}
			return true
		}
	}
	q.replies = append(q.replies, reply{server, view, t})
	return true
}

func (q *TxnQuorum) find(server uint32) (reply, bool) {
	for _, r := range q.replies {
		if r.server == server {
			return r, true
		}
	}
	return reply{}, false
}

// AllLeadersPresent: every involved leader has replied.
func (q *TxnQuorum) AllLeadersPresent() bool {
	if q.leaders == 0 {
		return false
	}
	for _, l := range q.leaders.Members() {
		if _, ok := q.find(l); !ok {
			return false
		}
	}
	return true
}

// LeadersAgree: all leaders present and on one (view, t).
func (q *TxnQuorum) LeadersAgree() (view, t uint64, ok bool) {
	if !q.AllLeadersPresent() {
		return 0, 0, false
	}
	first := true
	for _, l := range q.leaders.Members() {
		r, _ := q.find(l)
		if first {
			view, t, first = r.view, r.t, false
		} else if r.view != view || r.t != t {
			return 0, 0, false
		}
	}
	return view, t, true
}

// FastReady: leaders agree on (V,T) and ≥ quorum replies carry (V,T).
func (q *TxnQuorum) FastReady() bool {
	v, t, ok := q.LeadersAgree()
	return ok && q.matching(v, t) >= q.quorum
}

// LeadersDisagree: all leaders replied but disagree → slow path.
func (q *TxnQuorum) LeadersDisagree() bool {
	if !q.AllLeadersPresent() {
		return false
	}
	_, _, ok := q.LeadersAgree()
	return !ok
}

// LeadersMovedOff: all leaders agree on a t other than the request's own reqT (a late
// leader re-timestamped it) and the leaders alone are short of a quorum. Followers only
// SpecReply with reqT (on time) or stay silent (late), so no quorum can form at the
// leaders' t without a SlowCommit: waiting for the slow timeout gains nothing.
func (q *TxnQuorum) LeadersMovedOff(reqT uint64) bool {
	_, t, ok := q.LeadersAgree()
	return ok && t != reqT && q.leaders.Count() < q.quorum
}

// AgreedT is the common leader t (0 if they don't agree).
func (q *TxnQuorum) AgreedT() uint64 {
	_, t, _ := q.LeadersAgree()
	return t
}

// SlowAgreedT is the max over the involved leaders' t (the SlowCommit agreedT).
func (q *TxnQuorum) SlowAgreedT() uint64 {
	var m uint64
	for _, r := range q.replies {
		if q.leaders.Has(r.server) {
			m = max(m, r.t)
		}
	}
	return m
}

func (q *TxnQuorum) ReplyCount() int          { return len(q.replies) }
func (q *TxnQuorum) Leaders() types.ServerSet { return q.leaders }

func (q *TxnQuorum) matching(view, t uint64) int {
	n := 0
	for _, r := range q.replies {
		if r.view == view && r.t == t {
			n++
		}
	}
	return n
}
