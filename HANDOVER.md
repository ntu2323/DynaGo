# DYNA Go port — handover

This file hands the project over to a fresh Claude Code session that will write a **Go
implementation of DYNA**. A working, benchmarked C++ implementation already exists in this
repository. Treat it as the reference: copy its behaviour, not its code structure.

Repo root: `/Users/nathantu/research/anirudh/Dyna/` (not a git repo at the top level; `Ionia/`
is a git repo). Paths below are relative to the root.

---

## 1. Goal and scope

Build DYNA in Go. DYNA is a replication protocol for disk-based KV stores.
- Its ordering is deadline-based, like Nezha's, so commit latency does not depend on disk
  persistence.
- It supports multi-key, multi-partition transactions.
- Topology: N = 2f+1 servers. Every server stores all N partitions. Server i leads
  partition i and follows the rest.

Suggested milestones (each one green on its ported tests before moving on):
1. Types, the clock seam, OrderKey and the specLog priority queue (update-key).
2. Orderer: early/late buffers, release loop, re-timestamp, ApplyCommit, Fetch.
3. Client quorum (fast and slow path), replica message handling, UDP transport. End-to-end
   3-replica write test.
4. Replication (ReplicatePrepare/Reply/Commit), TsAgree, dependency-ordered executor.
5. Read path.
6. Recovery: VR layer, partition-leader recovery, install, leader-sync.
7. Rejoin (disk-based VR state, snapshot catch-up).
8. Performance work (lanes, batching): only after correctness. See §6.

**Out of scope, as in the C++ version:** RMW, thrifty multicast, network-partition handling,
and GC of the executed set and reply cache (Tthresh).

## 2. Sources of truth (read these first)

| What | Path |
|---|---|
| **Ground-truth algorithm** | `Ionia/protocols/src/ionia/dyna/dyna_algo_design.txt` (170 lines; read it fully). Its read-path line about a late follower sending a Reject-Reply is struck out in the original: the follower stays silent. |
| Implementation companion (data model, messages) | `Ionia/protocols/src/ionia/dyna/spec.md` |
| Project notes: architecture, recovery, invariants, perf history, env vars | `CLAUDE.md` (root) |
| C++ implementation | `Ionia/protocols/src/ionia/dyna/` (`server_orderer.{h,cc}`, `replica.{h,cc}`, `client.{h,cc}`, `view_coordinator.{h,cc}`, `recovery.{h,cc}`, `commit_quorum.h`, `topology.h`, `deadline.h`, `txn_types.h`, `indexed_min_pq.h`) |
| Wire messages | `Ionia/protocols/src/ionia/dyna/dyna-proto.proto` (package `specpaxos.dyna.proto`). Reusable with `protoc-gen-go`. |
| C++ unit tests (the behaviour spec) | `Ionia/protocols/src/ionia/dyna/tests/*.cc` |
| Nezha (the deadline/OWD idea) | `Nezha/` |
| Older outdated spec | `Dyna_Protocol_Spec.md` (background only) |

If the design doc and the C++ code disagree, the design doc wins. The exceptions are the
user decisions recorded in `CLAUDE.md`, e.g. local entries not in any M are kept at install,
and the "DECISION B" counter-veto was removed. Ask the user when unsure. **Don't change
protocol rules without the user's approval.**

## 3. Protocol summary (condensed; the design doc and CLAUDE.md have details)

**Identity and order.**
- A transaction has a single `OrderKey = (t, clientId, requestId)`, compared
  lexicographically.
- Every replica orders by this key (invariant 1).
- `TxnId = (clientId, requestId)`.
- Use a real hash for TxnId. The C++ framework's hash returned 0 for every key and silently
  made the maps O(n).

**Per-server state.** One ordering domain per server, not one per partition.
- A txn lives in exactly one of three structures:
  - `earlyBuffer` (a priority queue by OrderKey);
  - `lateBuffer` (a hash map);
  - `specLog` (an indexed min-PQ with update-key, because re-timestamping re-heapifies).
- Plus an `executed` set.

**Write path.**
1. The client picks `t = now + OWD bound` and multicasts the txn to all servers.
2. On arrival:
   - on time (`clock <= t`) → earlyBuffer;
   - late at a leader of any key in the write set → re-timestamp `t := clock()` (placed above
     `highestReleasedT`) and release immediately;
   - late at a pure follower → lateBuffer, silently.

   A txn already executed is never re-admitted.
3. **Release loop:** when the clock passes t, the txn becomes SPECULATIVE in specLog and the
   server sends `SpecReply {view, clientId, reqId, t}`.
4. **Client quorum.**
   - **Fast path:** f+1 replies that include every involved partition leader, all agreeing on
     `(view, t)` → committed. The client then sends `FastCommit` asynchronously.
   - **Slow path:** all leaders replied, but the leaders disagree, or matching replies stay
     short of f+1 within the slow timeout. The client sends `SlowCommit(max t)` and waits for a
     `CommitReply` quorum at the agreed t.
   - A missing leader reply → resend with the **same t**.
   - Quorums only count replies from the client's current view.
5. **ApplyCommit:**
   - Find the txn in specLog, else the early buffer, else the late buffer, else
     `FetchRequest` it from a peer.
   - Adopt the agreed t and mark it COMMITTING.
   - Never downgrade COMMITTED.
   - SlowCommit also triggers a CommitReply.
6. **Replication:** the min-id involved leader sends `ReplicatePrepare` once a txn is
   COMMITTING. Once it has f `ReplicatePrepareReply`s the txn is COMMITTED and the leader sends
   `ReplicateCommit`.
7. **Background tick:**
   - SPECULATIVE past a timeout → `TsAgree` among the involved leaders, take the max, mark
     COMMITTING and CommitReply the client.
   - Re-broadcast lost prepares.
   - A non-primary stuck in COMMITTING re-acks.
8. **Execution:** an executor applies COMMITTED writes in dependency order. A write runs only
   when no smaller-OrderKey conflicting txn is un-executed. It then garbage-collects them.
   Values are stored as `[t:8 bytes LE][value]` so recovery can select by timestamp. There is
   no fsync on the commit path (invariant 2).

**Read path.**
- A leader reads its keys from the store once no smaller un-executed conflicting write
  remains, then replies with results.
- An on-time follower endorses immediately, without results and without logging.
- A late read at a pure follower is dropped silently.
- The client merges per-leader results. On timeout it retries as a new request at a larger t.

**Recovery (two layers).**
- A silent VR layer runs heartbeats. A peer silent past the failure timeout triggers
  StartViewChange/DoViewChange. The VR leader is the min-id alive server.
- A view = (id, alive set).
- Normal processing pauses during the change.
- A dead server's keys move by **consistent hashing with virtual nodes**; survivors keep their
  ranges.
- Each survivor is the partition leader (PL) for its new range delta-p:
  1. It gathers f+1 `RecoveryReply`s (its own included) into M. M holds the txns touching
     delta-p, with status SPEC/COMMITTING/COMMITTED, plus executed key-values with their t.
     Merge precedence: COMMITTED > COMMITTING > SPEC, then max t; for key-values, max t.
  2. It sends `RecoveryPrepare` and collects f+1 `RecoveryPrepareReply`s.
  3. It sends `RecoveryReport` to the VR leader.
  4. Once every survivor has reported, the VR leader sends `StartView`.
- **Install:**
  - Key-values are written t-guarded.
  - Txns are adopted without downgrade.
  - Local entries not in any M are kept.
  - Then every inherited SPECULATIVE write the server now leads is leader-synced
    **immediately**. On any TsAgree-driven SPEC→COMMITTING, the leader sends the client
    `ViewInfo` (if the txn predates the view) and `CommitReply`.
  - Replicas keep a `clientId → address` table, because M carries no addresses.
- Clients on an old view get `ViewInfo` and restart their pending requests in the new view;
  writes keep the same t.
- If the VR leader dies mid-change, recover from the OLDEST last-normal view (VR's
  most-behind-wins rule).

**Rejoin.**
- Each server keeps `(view, alive)` fsync'd in `dyna-vr-<idx>.state`.
- If a server finds that file at startup, it starts JOINING:
  1. JoinRequest → JoinInfo.
  2. It applies live replication as a non-member.
  3. It pages a snapshot from the VR leader (`SnapshotRequest`/`SnapshotChunk`).
  4. JoinReady → a normal view change adds it back, and consistent hashing returns its home
     range.
- If all servers are JOINING for the grace period, the cluster starts fresh in view 0.
- The store is not trusted across a restart.

**Critical invariants.**
1. Ordering is identical everywhere: sort by OrderKey.
2. Disk is never on the commit path: commit means f+1 replicas agree in memory.

**Commit rule.** Leader replies are mandatory; a follower-only quorum never commits.

## 4. Conformance tests to port first

The C++ suites drive the logic with a **mock clock** through a `WallClock` seam
(`wall_clock.h`). Keep that seam in Go: an interface `Clock { Now() uint64 }` plus a mock you
advance by hand. The orderer must not read `time.Now()` directly. Port each suite as Go table
tests. Test names (in `Ionia/protocols/src/ionia/dyna/tests/`):

- **types_test** — OrderKey ordering and ties, TxnId uniqueness, proto round-trips, mock clock, TxnMap, batch-outbox fill rules.
- **indexed_pq_test** — push/peek/pop order, duplicate reject, erase arbitrary, update-key up/down, randomized vs an oracle.
- **orderer_release_test** — ReleasesAtDeadline, ReleasesInOrderKeyOrder, OnTimeTxnBehindHighestReleasedStillReleases, BackgroundLoopReleasesAndStops, ExternalDriverHintTracksEarliestDeadline, LateLeaderRetimestampAboveOtherLanes.
- **orderer_commit_test** — LateLeaderReTimestampsAndReleases, OnTimeLeaderTxnBehindHighestReleasedKeepsItsT, LateFollowerBuffersSilently, ApplyCommit from specLog / early / late, CommitNeedsFetchThenInstallThenApply.
- **quorum_test** — single- and multi-partition fast path, missing leader blocks, leader disagreement → slow, slow path re-forms the quorum at the agreed t, stale follower reply, other-view replies ignored.
- **slowpath_test** — leader disagreement resolves to the max; a follower adopts the agreed t from the lateBuffer.
- **tsagree_test** — two leaders converge to the max; a single leader commits immediately; nothing happens before the timeout.
- **replicate_test** — the primary leader drives COMMITTED; only the primary drives; a prepare adopts a held lateBuffer entry; the budgeted scan picks the smallest OrderKeys first.
- **exec_test** — WW conflicts execute in OrderKey order; non-conflicting txns run past an uncommitted smaller one; applied with the commit t; the dependency rule holds; cross-lane conflict order; LaneOf is stable.
- **read_test** — a leader read returns the committed value after the write executes; a read is blocked by an un-executed conflicting write; a late follower drops the read; an on-time follower endorses; an on-time leader read is held until executed.
- **robust_test** — commit overtakes a buffered txn; re-heap adopts the right entry; never downgrade COMMITTED; executed txns are never reinstalled; a lost TsAgree / ReplicateCommit still converges.
- **storage_test** — value+t codec round-trip, foreign-blob decode, versioned apply/read.
- **recovery_test** (26 tests, the biggest):
  - ranges and the M merge;
  - inherited SPEC synced immediately (single and multi leader);
  - install never downgrades;
  - kv t-guard;
  - leftovers kept;
  - follower dies; VR leader dies;
  - message loss;
  - left-out node resets;
  - 5 servers with 2 sequential failures;
  - leader dies after a partial StartView;
  - every rejoin variant;
  - snapshot paging;
  - home range handed back;
  - whole-cluster restart.

  Its harness is an in-process simulated network with message loss. Build the Go equivalent
  early: a fake transport that routes between in-process replicas, can drop messages, and
  runs on the mock clock.

## 5. Suggested Go architecture

- **Packages:** `types` (OrderKey, TxnId, entries), `pq`, `orderer`, `quorum`, `topology`
  (consistent hash + vnodes), `vr` (view coordinator + recovery), `replica`, `client`,
  `transport`, `store`, `bench`.
- **Transport: UDP.**
  - The C++ version is UDP; it is *not* TCP.
  - Use `golang.org/x/net/ipv4.PacketConn.ReadBatch` / `WriteBatch`
    (recvmmsg/sendmmsg on Linux) and `SO_REUSEPORT` via `net.ListenConfig.Control`, with
    several receive goroutines each owning a socket.
  - One message per datagram, ≤ ~8–9 KB. The cluster has jumbo frames; anything bigger goes
    through the chunked bulk transfer (§6).
- **Concurrency: per-transaction lanes.**
  - Shard by `hash(TxnId) % K`. Each lane owns its slice of buffers, specLog, executed set,
    dedup/reply cache, OWD estimator and outbox.
  - A goroutine per lane is pinned with `runtime.LockOSThread`. Receive goroutines route each
    message to its lane over a channel or lock-free queue.
  - Cross-lane state:
    - the executor pass, a brief snapshot across lanes;
    - an atomic global `highestReleasedT` floor for late-leader re-timestamps.
  - Start with K=1 and keep K a parameter. Run the tests at K=1 and K=4.
- **Executor:** a separate goroutine that owns the store and is woken on COMMITTED events.
  It snapshots under the lane locks and does the sort, conflict scan and apply outside them.
- **GC risk (important for a deadline protocol).**
  - A GC pause or scheduler hiccup makes requests arrive late → slow path → resends. That is
    exactly the spiral that caps the C++ version.
  - Mitigations:
    - preallocate maps (the C++ version pre-reserves 8M entries);
    - pool message buffers with `sync.Pool`;
    - avoid per-message heap allocations on the hot path (watch protobuf unmarshal allocs;
      consider `vtprotobuf` or hand-rolled encoding);
    - tune `GOGC` / `GOMEMLIMIT`;
    - measure GC pause percentiles alongside OWD percentiles.
- **Store:** start with an in-memory map behind an interface. Later options are Pebble or
  Badger as a stand-in for SplinterDB, or cgo to SplinterDB. No fsync on the commit path.
- **Bench client:** open loop, Poisson or smoothed rate, a configurable number of client
  goroutines. FastCommits batched per replica (`FastCommitBatch`). Report the completed %,
  median and p99.
- **Config:** reading the existing config format lets the cluster scripts drive the Go
  binaries:
  ```
  f 1
  replica 10.10.1.1:13000
  replica 10.10.1.3:13000
  replica 10.10.1.4:13000
  ```

## 6. Lessons learned (pitfalls the C++ version hit; design for them from day one)

1. **The overload spiral.** Late arrival → slow path → more messages → deeper queues → more
   late arrivals. Past capacity DYNA collapses rather than plateauing. Every item below is
   partly about breaking this loop.
2. **OWD bound calibration.** The client deadline bound comes from an OWD percentile
   estimator: p50 over a window of 100, clamped (`DYNA_MAX_OWD_US`). Too tight means
   everything is late; too loose adds latency. `deadline.h` has the logic. Clock sync matters:
   chrony on the cluster gives ~1 µs offsets.
3. **Never hold the ordering lock across sends or disk I/O.** Send outside the lock.
4. **Background scans must not stall the hot path.**
   - Run them outside the big state lock (C++ `state_lock_` is writer-preferring; a long scan
     plus a waiting heartbeat stalled every lane).
   - **Budget** each scan's actions per tick (4096, smallest OrderKey first). Unbounded
     post-recovery prepare resends reached 2M/s.
5. **Timers must not share the receive loop.** With SO_REUSEPORT, every packet from one peer
   lands on one socket. A heavy tick on that socket's thread drops all of that peer's
   traffic. Run heartbeats and scans on their own goroutines.
6. **Large recovery messages (M ≈ 3–5 MB at load) must be chunked and acked.**
   - The C++ version sends ≤1300 B chunks, starting each round at a random chunk, and the
     receiver keeps them across resends.
   - A resend of an already-delivered xfer is a small "poke"; the receiver replays the
     answers it already produced.
   - Messages are built once per round and their chunks cached.
   - Sending one huge fragmented datagram never arrived under load.
7. **Client resends must be rate-limited and cheap.**
   - Use a token bucket (~1000 resends/s per client, oldest first), including the post-view
     change restart.
   - Track resend deadlines in a **heap**, not with an O(pending) sweep.
   - Keep slow-path checks in a separate queue so budget-starved resends don't block them
     (head-of-line blocking).
   - Per-request resend backoff goes up to 32×.
8. **Lane load shedding:** drop new ClientRequests when a lane queue exceeds ~8192 (the
   client retries); commits and replication keep flowing.
9. **Executor lock duty cycle:** the all-lanes snapshot must not run too often (interval
   ≥ 10 × the last snapshot hold); its cost is O(specLog).
10. **Message combining helps a lot.** FastCommitBatch from clients, SpecReplyBatch on replica
    senders (only for replies already queued, with no added wait), and batched receive. The
    cost is per-packet kernel work. Background replication and TsAgree go in `BgBatch`es per
    destination (flush ~500 µs or ~8 KB). Batching that *delays* replies made things worse.
11. **Bounded or GC'd state.** Latency drifted upward as maps grew and rehashed under locks.
    Pre-size the maps.
12. **Unit tests caught things the cluster could not.** Every behaviour change gets a
    mock-clock test.

## 7. Baseline numbers (CloudLab Wisconsin c220g2, 3 replicas + 3 client machines)

| System | Result |
|---|---|
| C++ DYNA, best config (4 lanes, 3 rx threads, 2 sender threads, busy-poll) | ~240–250k ops/s at 100%, median ~0.57–0.6 ms; ~265k median ~0.72 ms; 300k at 100% in some runs; 350k collapses |
| C++ DYNA failover (kill a replica, or kill + restart the VR leader) | ~120k ops/s: 100% completed, ~1 s dip; at ~200k it survives degraded (~60% of the pre-kill rate) |
| IONIA (baseline, tuned) | ~200k ops/s at the edge, median 0.26–0.47 ms |
| NOPaxos | 75.9k ops/s, median 201 µs |

**Gotcha:** `run_cluster_bench.py --offered-ops-per-sec` is **per sender machine** (3
senders). Arg 50000 ≈ 120k total.

## 8. Cluster and tooling

- Bench tooling lives in `Ionia/protocols/experiments/GCP_tests/`:
  - `run_cluster_bench.py --protocol dyna` (and `ionia`);
  - `run_throughput_sweep.py`;
  - `setup_dyna_cluster.py`, `prepare_cluster.py`;
  - `cluster_hosts_wisc.json` (current reservation);
  - `remote/` scripts (`setup_dyna.sh`, `tune_node.sh` = NIC IRQ steering + interrupt
    moderation off, `prepare_data_disk.sh`).

  The Go port has its own path: `setup_dyna_go_cluster.py` deploys it and
  `run_cluster_bench.py --protocol dyna-go` runs it. See the M8 runbook in §10.
- **Cluster access:**
  - SSH user and key are set through env vars before running the scripts
    (`SSH_USER`, `SSH_KEY_FILE`); ask the user for the values.
  - Node IPs are in `cluster_hosts_wisc.json`.
  - 10 physical cores on the NIC NUMA node; core pinning matters.
  - Kernel RTT between nodes is ~45–49 µs.
- The C++ code builds only in Docker (`docker/Dockerfile.udp`). Go needs no such setup: build
  a static linux/amd64 binary with `GOOS=linux GOARCH=amd64` and rsync it to the nodes.

## 9. Open questions for the user or advisor

1. **What does "the C++ TCP is slow" refer to?** DYNA's C++ transport is UDP
   (`SharedUDPPort`, recvmmsg, SO_REUSEPORT). Its measured bottlenecks were per-packet kernel
   work and thread and lock design, not C++. Is the motivation speed, code simplicity, or a
   different baseline (e.g. a Go-based system to compare against)? The answer changes what
   the port should optimize for.
2. Stay **wire-compatible** with the C++ `.proto` (mixed C++/Go clusters, C++ clients against
   Go replicas)? Recommended: yes. It's free with protoc-gen-go and gives a cross-check.
3. **Storage backend:** in-memory only for the first milestones, then which disk store?
4. Which **comparison target** matters for the paper: Go DYNA vs C++ IONIA, or vs a Go
   baseline?

---

## 10. Status / progress (Go port)

**User decisions (2026-10-27):**
- Port step by step, aiming for a full port that includes the perf work.
- **Go-native wire format**, not compatible with the C++ protobuf/UDP framing.
- **Throughput parity with C++** is the goal.
- Store: in-memory first, Pebble later.

Plan file: `~/.claude/plans/hello-my-advisor-wants-rosy-codd.md` (milestones M1–M8 as in §1).
Module `dynago`, Go 1.23. Run tests with `go vet ./... && go test -race ./...` from `dyna_go/`.

| Milestone | State |
|---|---|
| M1 Foundations | **done** (2026-10-27) |
| M2 Orderer core | **done** (2026-10-27) |
| M3 Client + replica + transport (E2E) | **done** (2026-10-27) |
| M4 Replication, TsAgree, executor | **done** (2026-10-27) |
| M5 Read path | **done** (2026-10-27) |
| M6 Recovery | **done** (2026-10-27) |
| M7 Rejoin | **done** (2026-10-27, together with M6) |
| M8 Performance + cluster | **local part done** (2026-10-27); cluster runs pending (no CloudLab nodes) |

### M1: what exists
- `internal/clock` — `Clock` (Now/SleepUntil/Interrupt; sticky interrupt), `Real` (timer or busy-poll), `Mock`.
- `internal/types` — `TxnId` (+ splitmix `Hash()` for lane sharding), `OrderKey` (`Less`), `Status`, `Txn`, `KV`, `TxnEntry`, and `ServerSet`.
  - `ServerSet` is a uint64 bitmask: N ≤ 64, matching the C++ alive sets.
  - `TxnEntry.PrepareReplies` is a `ServerSet` instead of a `std::set`.
  - `ClientAddr` is a `netip.AddrPort`: comparable and allocation-free.
- `internal/pq` — generic `IndexedMinPQ[ID, V]` holding `*V` values, so a sift swaps pointers. Supports update-key and erase, and returns the value from `Erase`.
- `internal/store` — `Store` interface, `Mem`, and the `[t:8 LE][value]` codec.
- `internal/deadline` — `ReplicaOwdEstimator` (per lane; not thread-safe) and `ClientBoundTracker`, including the congestion floor. Env names are the same as in C++.
- `internal/wire` — every message in `dyna-proto.proto`, encoded as a type byte followed by uvarint and length-prefixed fields.
  - `Marshal(dst, m)` appends; `Unmarshal(b)` copies strings, so receive buffers can be pooled.
  - Optional proto fields map to a pointer (`ReplicatePrepare.Txn`), a presence bool (`DoViewChange.HasLastNormalView`), or 0 meaning absent (`Txn.SendTime`).
  - `BulkChunk.Inner` is a `MsgType`, not a type-name string.
- `internal/outbox` — `BgOutbox`, the per-destination `BgBatch` accumulator. It's full by entry count or by encoded bytes.

**Tests ported:** `types_test`, `indexed_pq_test`, `storage_test`, plus `wire_test`, which replaces the proto round-trips and covers all 34 types and truncation. The `TestXxx_Name` names mirror the C++ `TEST(Xxx, Name)`.
- `DbBackend.SplinterDbWriteThenLeaderReadThroughExecutor` is deferred: it needs the executor (M4) and will run against Pebble in M8.
- The `BgOutbox` tests live in `internal/outbox`.

**Deviations from C++:** none in protocol behaviour. The codec is Go-native by design.

### M2: what exists
- `internal/topology`:
  - `Topology{N, Alive}` with `OwnerOf`, which uses consistent hashing over a 16-vnode ring when the home leader is dead.
  - `InvolvedLeaders` / `InvolvedPartitions` return a `ServerSet`.
  - `KeyForPartition` is a test helper.
  - **Deviation:** `PartitionOf` uses FNV-1a with a fold, whereas C++ uses `std::hash`. Key→partition placement therefore differs from C++. That is fine, since the two clusters never mix. The tests derive keys with `KeyForPartition`.
- `internal/orderer` (`orderer.go`):
  - K lanes from day one: `New(..., lanes)` with `0` meaning `DYNA_LANES`. `LaneOf` uses the same mix as C++. Each lane has its own `sync.Mutex`, early/spec `IndexedMinPQ`, late map and executed map, plus a lock-free `nextDue` hint.
  - Covers `SubmitTxn` (on time / late-leader re-timestamp above the global atomic floor / late-follower buffer / late follower READ drop / executed and specLog duplicates), the release loop (`Start(true)`, clock-seam park), `ReleaseDue` / `ReleaseDueLane` / `NextDueHint` for an external driver, `ApplyCommit`, `InstallFetched` and `TryGetBody`.
  - `ApplyCommit` returns `(CommitResult, CommitBrief)` rather than a full entry copy.
  - Callbacks get a *reply copy* pointer valid only during the call, from a per-lane reused buffer.
  - `computeLeadership` runs under the lane lock (C++ ran it before locking).
- `executor.go` is a stub that only coalesces `KickExecutor`. M4 fills it in.

**Tests ported:** all of `orderer_release_test` and `orderer_commit_test`, plus `DuplicatesAndLateFollowerRead`. They pass with `-race` at `DYNA_LANES=1` and `4`. The tests that assert global release order or the max `highestReleased` across lanes check those only at K=1, because per-lane draining interleaves lanes.

### M3: what exists
- `internal/quorum`: `TxnQuorum`, view-scoped. The latest reply per server wins, and it is stored in a slice rather than a map.
- `internal/config`: parses the C++ config format (`f`, `replica host:port`). `Resolve` unmaps 4-in-6 addresses.
- `internal/transport`:
  - The `Transport` interface: `Send`, `SendReplica`, and `Broadcast`, which excludes self.
  - `UDP`: one socket, one message per datagram, buffers pooled. It also unmaps 4-in-6 addresses, because without that a udp4 socket can't reply.
  - `SimNet`: in-process, encodes through the real codec, deterministic delivery via `Deliver`, plus `Drop` / `SetDown` hooks.
- `internal/replica`: handles ClientRequest (OWD sample, at-most-once reply cache plus in-flight set per lane, answering from live state), FastCommit / FastCommitBatch / SlowCommit (view check → ViewInfo), Fetch request/reply with pending commits, and CommitReply. Releases send their SpecReply inline from the orderer callback. Sender goroutines come in M8.
- `internal/client`:
  - The general API is `Invoke(Op{Reads|Writes}, cb)`: multi-key, pure read or pure write. The C++ bench client issues single-key transactions only.
  - Ported from C++: same-t write resends, the slow-path timeout `max(DYNA_SLOW_TIMEOUT_US, 3×bound)`, SlowCommit, CommitReply, reads retried as fresh ids at a doubling OWD, the token-bucket resend cap plus deadline heap and ready heap, FastCommit batching, and ViewInfo restarts.
  - `Tick()` drives the sweep, so mock-clock tests call it directly; `Start()` runs a ticker goroutine for real runs. Callbacks run outside the client lock.
- `internal/simcluster`: N replicas plus M clients on a SimNet with one mock clock (`Step`, `Run`). It is reused by the later milestones' tests.
- `cmd/dyna-replica` (`-c -i`) and `cmd/dyna-client` (`-c -j -e -w -n -rate -keys -value-size -reads -t`). The client prints `Completed N requests`, median/p90/p99 latency, and fast vs slow counts.
- `scripts/smoke.sh`: 3 replicas on localhost plus the bench client. It defaults to `DYNA_BUSY_POLL=0` and `DYNA_INITIAL_BOUND_US=2000`.

**Tests ported:** `quorum_test` (all 7) and `slowpath_test` (both, in `internal/orderer`).
- New `simcluster` E2E tests: fast-path write, slow path with fetch (followers lost the request), multi-partition leader disagreement (late re-timestamp → SlowCommit(max)), lost leader reply answered from the cache, and 60 writes converging on identical t.
- A UDP loopback test.

**Smoke results (Mac, localhost):**

| Variant | Throughput | Median | Notes |
|---|---|---|---|
| Closed loop, 4 clients, bound 2 ms | 1.8k ops/s | 2.2 ms | Every request finished |
| Tight bound (`DYNA_INITIAL_BOUND_US=50 DYNA_MAX_OWD_US=100`) | 15.9k ops/s | 0.16 ms | 2.7k slow-path timeouts, all committed |
| Open loop, 20k ops/s, 8 clients | 20k ops/s | 0.36 ms | p99 0.83 ms |

**Known gaps (by design until later milestones):** no VR or recovery; the view is fixed at 0 (M6).

### M4: what exists
- `internal/orderer/background.go`:
  - **TsAgree:** `DriveTsAgreement` (stuck SPECULATIVE we lead, or `SyncNow` → our proposal; a single-leader txn commits at once) and `RecordTsProposal` (answers any non-reply with our t, executed included).
  - **Replication:** `DriveReplication` (primary = min-id involved leader, throttled re-broadcast), `TakeReplicateNow`, `RecordPrepareReply` (f acks → COMMITTED; executed/COMMITTED → resend), `HandleReplicatePrepare` (adopts from specLog, early or late, or materializes from the body; never re-installs an executed txn), `MarkCommitted`, and `CollectStaleCommitting` (non-primary re-ack).
  - All three scans go through `scanBudgeted`: collect lane by lane, then keep the smallest OrderKeys up to `DYNA_SCAN_BUDGET`, and re-check under the lane lock.
- `internal/orderer/executor.go`:
  - An event-driven executor goroutine using a `sync.Cond` and a lock-free coalesced kick, with `DYNA_EXEC_MIN_INTERVAL_US` and the 10×-snapshot duty-cycle rule. `RunOnStoreThread` is in place for recovery.
  - `ExecuteReady` follows the C++ logic: (1) an all-lanes snapshot of (OrderKey, pointer); (2) an unlocked sort plus a single ascending blocked-key pass; (3) per-lane erase plus `executed` bookkeeping; (4) apply in OrderKey order.
  - The apply respects `recoveredT` and records `writtenKeys`. Leader reads are served from `LedKeys`, with results passed to the reply callback.
- `internal/replica/background.go`:
  - `Tick()` runs the three scans plus an executor kick; `leaderSyncDone` sends ViewInfo if needed plus a CommitReply, using the client-address table.
  - Handlers cover replicate prepare, reply and commit, TsAgree, and BgBatch (prepares before commits).
  - The per-lane `BgOutbox` covers prepare, ack, commit and TsAgree (`DYNA_REPL_BATCH`, `DYNA_REPL_FLUSH_US`, `DYNA_REPL_MAX_BYTES`); `FlushBg()` is called on a timer.
  - In real mode the replica starts two ticker goroutines, one for the scan and one for the background flush. `Options.ExternalDrive` (simcluster) starts no goroutines, so the test drives release, execution, flush and ticks deterministically.
- **UDP fix:** socket buffer sizes are requested with fall-back (32 MB, shrinking ×3/4 on failure). A 16 MB request failed outright on macOS, which caps at 8 MB, and left the default ~780 KB buffer. That caused hundreds of thousands of drops and an occasional overload spiral in the smoke.

**Tests ported:** `tsagree_test` (3), `replicate_test` (4), `exec_test` (7) and `robust_test` (8). The read-after-commit executor test is part of `robust_test`.
- Two new simcluster E2E tests:
  - every Fast/SlowCommit lost → leader sync (TsAgree), CommitReply to the client, replication, execution;
  - 30% of replication traffic lost → re-broadcast and re-acks still converge with identical stores.
- The earlier E2E tests now assert execution on every replica, with the store holding the value at the agreed t.
- Everything passes with `-race` at `DYNA_LANES=1`/`4` and with `DYNA_REPL_BATCH=1`.

**Smoke (Mac):**
- Closed loop: 7,574 txns, executed on all 3 replicas, final `speclog=0`.
- Open loop at 20k ops/s, 8 clients: median ~0.40 ms, p99 ~2.2 ms, ~1–1.5% slow-path timeouts, zero socket-buffer drops, 4/4 runs healthy.

**Open item for M8:** a rare max latency of ~30 ms in the open-loop smoke. It isn't write resends (`resendW=0`); the likely cause is a SlowCommit re-send or scheduler/GC stalls on the laptop. Investigate it with GC-pause and OWD stats on the cluster.

### M5: what exists
Most of the read path already landed with M4:
- the executor serves leader reads from `LedKeys` once conflicting smaller writes have executed;
- on-time follower reads are endorsed at release, without logging;
- late follower reads are dropped;
- the client merges leader results and retries as a fresh request id at a doubling OWD.

**Tests ported:** all of `read_test` (5).
- E2E tests: read-after-write plus a two-partition merged read plus a never-written key; a read waiting for an un-executed conflicting write (it returns the new value, never the old); a lost read request → `RetryRead`.
- Smoke flag `SMOKE_READS=<fraction>`.

**Smoke at 20k ops/s with 30% reads:** median 0.45 ms, p90 2.3 ms, p99 12 ms. The read tail comes from leader reads waiting for an executor pass (`DYNA_EXEC_MIN_INTERVAL_US=2000`) and for conflicting writes, which the C++ version has too. About 0.6% of reads retried.

### M6 + M7: recovery and rejoin
- `internal/recovery`: `Data{Txns []wire.RecTxn, KVs []wire.RecKV}`, `Merge` (status precedence, then max t; kv max t; output sorted, so it is deterministic), `InDeltaP`, `InFailedRange`, `TxnTouches`.
- `internal/vr`: `Coordinator` is a line-by-line port of `ViewCoordinator`, including every rejoin state, as pure logic with `Effects{Out, BeginRecovery, Install, Excluded, AdoptView, FreshStart}`. `RecoveryPrepare` is built once per round and re-sent as the same object, so the bulk chunk cache hits.
- `internal/orderer/recovery.go`: `CollectRecovery`, `InstallView`, `installRecoveredLocked`, `applyKVsGuarded`, `Reset`, `SnapshotKeys`, `SnapshotRead`, `CommittedTxns`, `InstallSnapshot`.
  - `InstallView` installs t-guarded kvs plus `recoveredT`, never downgrades, keeps leftovers (counted), and marks `SyncNow`.
  - `ReleaseDue` now returns 0 while paused.
- `internal/replica/vr.go`:
  - **Locking:** `stateMu`, an RWMutex, is shared for hot messages and exclusive for VR. The `paused` atomic drops client traffic early.
  - **VR loop:** `HeartbeatTick` (`DYNA_HB_MS`; also the missed-view probe on a higher-view heartbeat) and `applyEffects` (pause, exclude→reset, adopt→catch-up, fresh start, begin recovery, install).
  - **Recovery:** the RecoveryReply is cached per PL; `installNewView` clears inflight and pending commits, persists the VR state, unpauses, and runs `runTsAgreement` immediately.
  - **Bulk transfer:** `sendVr` sends anything over `DYNA_BULK_MIN_BYTES` as acked `BulkChunk`s of `DYNA_BULK_CHUNK_BYTES`. It starts at a random chunk, uses an LRU of 4 encoded objects, and sends a poke instead once acked; the receiver replays captured answers, and `DYNA_BULK_TEST_DROP_PCT` drops chunks for tests.
  - **Rejoin:** `dyna-vr-<idx>.state` is written temp + fsync + rename in `Options.VRStateDir` / `DYNA_VR_STATE_DIR`. JOINING replicas apply live replication unacked; snapshot paging is done by sorted `writtenKeys` (`DYNA_SNAPSHOT_CHUNK`), and the last chunk carries COMMITTING/COMMITTED txns.
- **Deviations:**
  - The VR layer reads the replica's `Clock` (a mock in tests) instead of the wall clock directly.
  - The bulk xfer id is FNV-64 of the encoded bytes mixed with the sender.
  - Snapshot chunks go through `sendVr`, so they can be bulk-chunked: Go UDP has no fragmentation layer.

**Tests ported:** all 27 of `recovery_test`.
- `internal/recovery`: topology split and merge.
- `internal/orderer/recovery_test.go`: 10 RecoveryOrderer tests (FastCommittedLeaderDead runs both parameter values) plus 3 rejoin tests at orderer level.
- `internal/vr`: 14 FakeNet tests (RecoveryView ×7, Rejoin ×7).
- **New simcluster E2E tests** (`recovery_test.go`), each with real replicas plus clients on SimNet:
  - follower dies and VR leader dies, under load (before/in-flight/after writes all complete, stores identical);
  - a fast-committed write survives its leader's death at its t;
  - kill + restart → view 2, catch-up, home range back (write to it is fast);
  - bulk transfer with tiny chunks and 30% chunk loss;
  - whole-cluster restart → fresh view 0.
- `simcluster` gained `Kill`, `Restart`, `Heartbeat`, a per-cluster temp VR dir, and `SIM_VERBOSE=1` for VR logs.

**Smoke (Mac, UDP):** `SMOKE_KILL_REPLICA=N [SMOKE_RESTART_AFTER_MS=ms] scripts/smoke.sh` (kill −9 at 1.5 s into the run).

| Variant | Result |
|---|---|
| Kill r2, closed loop | view 1 on survivors; all writes executed; max latency ≈ 1 s (the failure timeout) |
| Kill + restart r2 | view 2 everywhere |
| Kill + restart r0 (the VR leader) | view 2 everywhere |
| Kill + restart r1 under 10k ops/s open loop | 100% of 80k completed; the rejoiner caught up 33k keys and view 2 installed ~1.2 s after view 1; p99 ≈ 0.9 s (failover window) |

### M8: performance (local part; cluster validation pending)

There were no CloudLab nodes, so everything below was built and measured on the Mac:
- **macOS UDP:** about 4.8 µs per `sendto` on loopback, so the Mac caps the setup near 40–50k ops/s whatever the replica does.
- **Linux container** (`scripts/docker-smoke.sh`): native arm64 Linux in Docker's VM, 10 vCPUs shared by all three replicas and the client.

Absolute numbers will differ on c220g2; the before/after deltas are what matter.

**What was built**
- **Transport (`internal/transport/udp.go`, `rx_linux.go`, `reuseport.go`):**
  - `UDPOptions{Sockets, RxBatch}`: N sockets share one port via `SO_REUSEPORT`, each with its own receive goroutine. Every socket must be read, because the kernel may deliver to any of them.
  - Receive on linux/amd64 and linux/arm64 is a hand-rolled `recvmmsg` over `RawConn` that decodes source addresses in place. x/net's `ReadBatch` allocated a `UDPAddr` for every datagram. Other platforms use x/net.
  - `BatchWriter` sends each round with one `sendmmsg`, through x/net `WriteBatch`; on darwin it loops, one message per call.
  - `Broadcast` is one `sendmmsg`.
  - `transport.WriterFor` falls back to per-message sends for SimNet.
- **Replica pipeline (`internal/replica/pipeline.go`)**, on by default (`DYNA_ORDER_THREAD=1`) unless `ExternalDrive`. It mirrors the C++ ordering thread, lanes and senders:
  - **Receive goroutines** decode and route each hot message to its txn's lane queue, an MPSC swap-slice. They split `FastCommitBatch` and `BgBatch` per lane, and handle VR messages inline under the exclusive `stateMu`.
  - **Lane goroutines** (`DYNA_LANES`) run their handlers under one shared `stateMu` hold per drained batch, then call `ReleaseDueLane`, then park until the next deadline or message.
    - In `DYNA_BUSY_POLL=1` mode they spin, with a `Gosched` every 16 spins. Otherwise they spin for `DYNA_SPIN_US`, then block.
    - The orderer wake hook (`SetWakeHook`) wakes lanes on resume, install and stop. A 50 ms backstop covers a missed wake.
  - **Senders** (`DYNA_SEND_THREADS`; lane i → sender i % S). SpecReplies already queued for the same client leave as one `SpecReplyBatch` (`DYNA_REPLY_COMBINE`). About 97% of replies were combined at 110k.
  - **Lane shedding:** `DYNA_LANE_SHED_QLEN` (8192) drops new `ClientRequest`s.
  - `cmd/dyna-replica` opens `max(DYNA_RX_THREADS, DYNA_SEND_THREADS+1)` sockets, with `DYNA_RX_BATCH` (64) datagrams per receive call.
- **Allocation and GC:**
  - **`internal/hmap`:** an open-addressing map for TxnId keys (linear probing, backward-shift delete), about 1.8× faster than a Go 1.23 map on the hot pattern. It replaces the orderer's `executed` and `late` maps and the replica's `replyCache` and `inflight` maps. `DYNA_PRERESERVE` now also pre-sizes the reply cache.
  - **`IndexedMinPQ`:** heap nodes carry their own index, with a free list, so sifts no longer touch a map (`pq.swap` was the top map user). The id index is an `hmap`.
  - **Codec:** `Marshal` and `Unmarshal` dispatch through a concrete type switch, so `Writer` and `Reader` stay on the stack and `Marshal` no longer leaks the message. A test asserts that `Marshal` allocates 0 times and `Unmarshal` once (the message itself).
  - Message literals on the reply and replication paths no longer escape: `sendSpec` takes a value, and the `bg*` fallbacks copy.
  - Replica allocations at load dropped by about a third. What remains is mostly txn strings, which are inherent.
  - The replica and client binaries default to `GOGC=400` unless `GOGC` is set. `GOMEMLIMIT` and `GOMAXPROCS` are passed through by the bench script.
- **`DYNA_STATS=1`:** a per-second `rX stats:` line with:
  - CPU and receive mix, shed count;
  - arrivals on time vs late at a leader or follower, mean slack and lateness, releases;
  - executor passes, writes, pass time, max snapshot hold, and skipped-older count;
  - buffer sizes;
  - per-lane busy %, messages and max batch; per-sender busy %, datagrams and combined %;
  - GC rate, GC CPU %, max pause bucket and live heap.

  The client prints its own CPU use, `resendSlowCommit`, and a count of requests over 25 ms.
- **Pebble store (`internal/store/pebble.go`):**
  - The WAL is off and writes use `NoSync`, matching the C++ SplinterDB `use_log=0`. Values are versioned.
  - The executor stages each pass's writes in one batch (`store.Stager`) and flushes before any leader read in the pass.
  - Selected with `DYNA_STORE=pebble` or the bench scripts' `IONIA_DB_MODE` (anything but `memory`). The directory is `$DYNA_DB_DIR/dyna-pebble-<idx>` and is wiped at startup.
  - Ports `TEST(DbBackend, …)`, both tests, plus a same-pass write+read test.
- **Profiling:** `dyna-replica -cpuprofile/-memprofile`, `DYNA_PPROF=host:port` (net/http/pprof), `dyna-client -cpuprofile`, and `SMOKE_PROFILE=1`.

**Two bugs found (both also in the C++ code)**
1. **Store divergence. This needs approval: it changes execution, not the commit rule.**
   - A replica can execute a COMMITTED write before a smaller-t conflicting write that it doesn't yet hold in its specLog: either late at a follower (lateBuffer), or due in a lane that hasn't run yet.
   - When that smaller write later commits at its own t, it is applied on top, and the replicas' stores diverge. The design doc's dependency rule sees only known txns.
   - The pipeline test caught it: about 1 run in 3 under `-race` with 4 lanes ended with different final values.
   - **Fix:** a t-guard (Thomas write rule) at apply. Execution never overwrites a key's value with an older OrderKey, which generalizes the old `recoveredT`. Writes are blind (RMW is out of scope), so stores converge.
   - **Residual:** a leader read served before the late smaller write was applied doesn't see it.
   - `DYNA_EXEC_T_GUARD=0` restores the C++ behaviour. Test: `TestExec_LateSmallerWriteNeverOverwritesNewerValue`.
   - **C++:** `InMemoryBackend::apply` / `DbBackend::apply` have no t check.
2. **~30 ms latency tail (the open item from M3).**
   - The client's sweep recomputes the slow timeout, `max(DYNA_SLOW_TIMEOUT_US, 3×bound)`, when the timer fires.
   - If the bound grew after the timer was armed (every OWD sample can move it), the check fell through to the resend case and waited until `send + 30 ms`.
   - **Fix:** re-arm at the new slow deadline. Over-25 ms requests at 30k went from 1,075 to 0–94. Test: `TestClient_SlowCheckRearmsWhenBoundGrows`.
   - Same code in C++ `client.cc:285`.

**Measurements (Linux container, open loop, 12 client goroutines; 3 replicas + client on 10 shared vCPUs)**

| Config | Clean up to | Edge |
|---|---|---|
| Before M8 (one release loop, inline sends) | 70k | 90k collapses (80k completed, p99 1.1 s) |
| Pipeline, 1 lane, 1 sender | 90k (median 2.5 ms, p99 6.7 ms) | 110k: 105k completed, p99 ~75 ms |
| + hmap / PQ / codec / recvmmsg | 110k (100% completed, p99 ~34 ms) | 130k: 118k completed |
| + GOGC=400 | 110k | 130k: 111–115k completed, median 2.6 ms |

At 130k the replicas are **not** CPU-bound: about 110% CPU each, lanes about 50% busy, senders 7%. The collapse begins with a ~30 ms stall of the whole process: GC pauses were ≤ 2.6 ms, and lane batches reached 3k messages. That looks like the oversubscribed VM descheduling parked goroutines, which dedicated cluster cores shouldn't show. The deadline bound is 2 ms in the smoke. On the Mac, macOS loopback syscalls, not DYNA, limit the setup to about 40–50k.

Failover smokes in the container through the pipeline at 20k ops/s:

| Variant | Result |
|---|---|
| Kill r2 | view 1 |
| Kill + restart r0 | view 2 everywhere |
| Kill + restart r1, 4 lanes / 2 senders | view 2 everywhere |

`go test -race ./...` passes at `DYNA_LANES=1` and `4`. The transport, replica, simcluster and orderer test binaries also pass inside the Linux container. The new `TestPipeline_UDPClusterWritesCommitAndConverge` runs 3 UDP replicas in pipeline mode with 4 lanes, 2 senders and 2 sockets.

**Cluster runbook (when nodes are back)**
```bash
cd Ionia/protocols/experiments/GCP_tests
# 1. Deploy (cross-compiles static linux/amd64; no toolchain on the nodes; also writes
#    ~/bench/dynaconfig + trace.micro like setup_dyna_cluster.py). tune_node.sh as before.
SSH_USER=... SSH_KEY_FILE=... python3 setup_dyna_go_cluster.py --hosts-file cluster_hosts_wisc.json
# 2. Same bench as C++ (offered rate is PER SENDER; 3 senders):
DYNA_LANES=4 DYNA_RX_THREADS=3 DYNA_SEND_THREADS=2 IONIA_DB_MODE=memory \
  SSH_USER=... python3 run_cluster_bench.py --protocol dyna-go --hosts-file cluster_hosts_wisc.json \
  -e 30 -w 5 --interval-ms 1000 --offered-ops-per-sec 80000 ...   # (same args as the C++ runs)
python3 aggregate_bench_results.py results/<run>
# Failover: --kill-replica 1 --kill-at-sec 10 [--restart-at-sec 15]
```
- **Binaries and processes:**
  - The Go binaries sit at `~/dyna_go/bench/{replica,client}`, so the bench scripts' `pkill '[b]ench/…'` patterns, log paths (`~/bench/replica-<idx>.log`), failover kill and view-summary markers all work unchanged.
  - `start_dyna_go_replica.sh` sets the C++ start script's defaults plus `DYNA_PRERESERVE=8000000`, and runs under `taskset` on the NIC NUMA node's CPUs with a matching `GOMAXPROCS`. Go can't pin individual goroutines.
  - The run dir is `/mydata/dyna` when `/mydata` exists. Cleanup wipes `dyna-pebble-*` and `dyna-vr-*.state`.
- **Client:** it accepts the C++ `bench/client` flags (`-c -s -m -k -n -e -w -t -j -i -l -O -G -R -F`) and prints the same lines the aggregator parses.
- **Edits outside `dyna_go`, all in `GCP_tests`:**
  - `run_cluster_bench.py`: the `dyna-go` protocol, and `DYNA_PROTOCOLS` for the shared dyna handling.
  - `gcp_bench_common.py`: cleanup for `dyna-go`.
  - `run_throughput_sweep.py`: the protocol choice.
  - `setup_dyna_go_cluster.py`: new.
  - `aggregate_bench_results.py`: untouched.

**First things to check on the cluster**
1. Run `DYNA_STATS=1` at 200–250k. Find which is near 100% (lanes, senders, receive goroutines) and compare against the C++ thread busy numbers in §7.
2. Busy-poll vs `DYNA_BUSY_POLL=0` with `DYNA_SPIN_US=50`. Go spinners hold Ps, so check `GOMAXPROCS` headroom: lanes + senders + rx + executor + GC.
3. Check the GC lines (`maxPause`, `gcCPU`). If pauses land in the deadline budget, raise `GOGC` or set `GOMEMLIMIT` high.
4. Count datagrams per op. Replies are combined; the client's 3 request sends are one `sendmmsg` per `Broadcast`.

**Not done / ideas**
- **Lazy body decode:** a prepare's txn body is decoded even when the follower already holds it. That's the largest remaining allocation source, but the C++ always sends the body too.
- **Client per-second stats line:** the C++ `DYNA client stats client=… completions=…` isn't emitted. It's an optional aggregator diagnostic.
- **Lane-pinned OS threads** (`runtime.LockOSThread` + `sched_setaffinity`): only worth trying if the cluster shows scheduler jitter.

### M8 cluster round 1: CloudLab Utah m400 (2026-10-27)

**Hardware:** 6× m400 (8-core aarch64 X-Gene, 62 GB, NIC `enp1s0d1`, no spare disk), much weaker than the c220g2. Node-to-node RTT is 72–95 µs; chrony agrees within 3.2 µs.
- Hosts file: `GCP_tests/cluster_hosts_utah.json`. Login: `nathantu` with `~/.ssh/corfu_remote`.
- Prep: `prepare_cluster.py --skip-disk`.
- The C++ built natively from a slim copy of the local tree, including uncommitted DYNA changes, via `setup_dyna_cluster.py --rsync-from` (9 min).
- Go was deployed with `setup_dyna_go_cluster.py --goarch arm64`.

**Common settings:** `IONIA_DB_MODE=memory`, `DYNA_PRERESERVE`, epoll clients (`DYNA_CLIENT_BUSY_POLL=0`), `-e 20 -w 5`, 3 senders. "Load" means completed ops/s in total. The C++ client offers about 2.5× the per-sender arg; the Go client offers exactly 3×.

| System / config | Clean (100%) | Edge |
|---|---|---|
| C++ DYNA, 3 lanes / 2 rx / 1 sender, busy-poll, `-j 4` | 45k (2 of 3 runs; median 1.2–2.9 ms), 47.5k once | 50k collapses at `-j 4` |
| C++ DYNA, same, `-j 8` | 50k (median 4.7 ms) | 55k collapses |
| Go DYNA, 3 lanes / 2 rx / 1 sender, `DYNA_BUSY_POLL=0 DYNA_SPIN_US=20`, `-j 4` | **72k (3 of 3; median 0.78–1.1 ms)** | 84k passes 2 of 3 (median 1.2–1.4 ms); 90k collapses |

**What it took (in order)**
1. **Busy-poll is wrong for Go on 7 usable cores.**
   - Spinning lanes and senders hold Ps. The execution trace showed runnable goroutines (executor, receive, heartbeat tick) waiting a total of about 6.7 s per 2 s to be scheduled.
   - Heartbeat ticks went 300 ms+ without running, which caused false view changes.
   - Full blocking (`DYNA_BUSY_POLL=0`, no spin) pays futex wakes instead.
   - Spinning 20 µs, then parking (`DYNA_SPIN_US=20`), is the sweet spot.
   - Clock reads (`time.Now`) are expensive on this ARM part, and the spin loop was 96% of them.
2. **Per-message, not per-batch, `stateMu` read lock in lanes.**
   - With the read lock held per batch, each heartbeat's exclusive lock (about 150/s) waited for the longest in-flight lane batch. Because the RWMutex is writer-preferring, every lane stalled behind it: lane iterations of 1.5–3 ms against about 300 µs of deadline slack.
   - A burst made everything late, clients resent with the same t, and the collapse hit all replicas in the same second.
   - After the fix, lane busy at 45k fell from 47% to 31%. The ceiling went from 42k to 72–84k.
   - **The C++ may have the same convoy:** hot messages hold a shared `state_lock_` per batch.
3. CommitReply and FetchReply now go through the sender goroutine, not a blocking `sendto` in the lane.
4. `DYNA_PRERESERVE` dropped to 2M. 8M made every reply-cache and executed-set insert a cache/TLB miss on these cores.
5. **Stall detectors in `DYNA_STATS`:** per-lane `maxIterUs`, `maxTick`, `maxHb`. The bench forwards `DYNA_PPROF` / `DYNA_PPROF_LOCKS` (replica), `CLIENT_PPROF` and `CLIENT_GOGC` (Go client). The Go client's finish line now reports `submitted=` for the measured window only, like C++.

**At 84k (Go):**
- Replicas are about 540% CPU, lanes about 40% busy, the single sender 75% busy (about 50k datagrams/s).
- About 80% of requests arrive on time; the rest take the slow path.
- Clients use about 3.5 cores each.

Next levers: deadline/OWD calibration (late arrivals), a second sender (one run with 2 senders + 3 rx collapsed, so not yet conclusive), and client CPU.

**IONIA on the m400s** (`setup_ionia_cluster.py --skip-build`, in-memory, batch 256, per-thread sockets, `-j 4`):

| Durability threads | 30k | 35k | 40k | 45k |
|---|---|---|---|---|
| 6 | 100% (median 1.1 ms) | 1 of 3 clean (others: panic; hang) | 2 of 3 clean (median 1.0–1.8 ms; one panic) | saturated: 85–92%, median 300–400 ms |
| 4 | — | 61% (hang) | 1% (panic) | 71% |

- **IONIA bug on aarch64, panic:** `PANIC UpdateClientTable (replica.cc:618): Assertion 'entry.lastReqId <= req.clientreqid()' failed`, preceded by "Ignoring stale request 1 < 2902004". That's a client-table entry claiming ~2.9M requests early in a fresh run, so it looks corrupted. It occurred in 3 of ~13 runs.
- **IONIA bug on aarch64, hang:** every client thread's completions drop to 0 mid-run and stay there, with no panic, in ~2 of 13 runs.
- Both look like data races that x86's stronger memory ordering hides. The same code was clean at 200k on c220g2.
- IONIA's capacity on these nodes, when it doesn't fail, is about 40k: less than C++ DYNA (~50k) and Go DYNA (72–84k).

**Failover on the m400s** (`run_cluster_bench.py --kill-replica N --kill-at-sec 10 [--restart-at-sec 15]`, `-e 30`; Go config as above; C++ 3 lanes / 2 rx / 1 sender; loads are total ops/s):

| Scenario | Go 12k | Go 36k | Go 60k | C++ 12k | C++ 36k |
|---|---|---|---|---|---|
| Kill follower r1 | ✅ 100%; view 1 ~1.06 s after the kill (the 1 s failure timeout), one ~1 s dip then catch-up | ✅ 100%, same shape | ✅ view 1, but the two survivors hold only ~20–24k against 60k offered (21–37% completed) | ✅ 100% | ✅ view 1, survivors ~18–20k (56%) |
| Kill VR leader r0, restart at +6 s | ✅ 100%; caught up (1000 keys, 24 txns) and view 2 ~5 s after restart | ✅ 100% (1000 keys, 156 txns) | ❌ catch-up of 86k txns finishes, but the overloaded survivors stop answering heartbeats and view 2 never installs | ✅ 100% | ❌ same failure (26k txns caught up, then a stall) |
| Kill r2, restart (home-range handback) | ✅ 100%, view 2 | — | — | — | — |

- **Fixed during this round: background scans walked the whole specLog under the lane lock.** After a kill at 60k the specLog grows to tens of thousands of entries. Three scans per 50 ms tick each walked every lane under its lock (`maxTick` 100–600 ms, lane iterations 0.2–1.3 s), and throughput decayed to ~1k ops/s.
  - `scanBudgeted` now collects candidates 512 entries per lock hold (`pq.At`), then sorts, dedups, and applies the budget.
  - The survivors now sustain ~20–24k at 60k offered.
- **Still failing:** kill+restart when the offered load is far above what two replicas can commit (60k offered against ~22k). The backlog grows without bound (specLog 350k, executor passes ~0.5 s), and the rejoin's view change can't complete. The C++ fails the same way, already at 36k. A fix would be admission control while degraded: shed client requests by specLog size or commit rate, not only by lane queue length.
- **Harness fix:** `gcp_bench_common.py` cleanup now deletes stale `~/bench/*.killed.log`. A previous kill+restart run's pre-kill log was being fetched again and failing the next run's check.

Not run this round: Pebble on the cluster.

