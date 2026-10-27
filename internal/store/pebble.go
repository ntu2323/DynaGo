package store

import (
	"errors"
	"log"
	"sync"

	"github.com/cockroachdb/pebble"
)

// Pebble is a disk-based Store (C++ DbBackend over SplinterDB). Like the C++ config
// (use_log=0, no fsync), the write-ahead log is off and writes are not synced: commit
// never waits on the store, a committed txn already lives on f+1 replicas, and the store
// is not trusted across a restart (rejoin catch-up refills it). Values are stored
// versioned ([t:8 LE][value]).
type Pebble struct {
	db    *pebble.DB
	buf   sync.Pool
	batch *pebble.Batch // staged writes (Stager; the executor goroutine only)
	enc   []byte
}

// OpenPebble opens (creating) a store in dir with a block cache of cacheMB.
func OpenPebble(dir string, cacheMB int) (*Pebble, error) {
	cache := pebble.NewCache(int64(max(cacheMB, 8)) << 20)
	defer cache.Unref()
	db, err := pebble.Open(dir, &pebble.Options{
		DisableWAL:   true,
		Cache:        cache,
		MemTableSize: 64 << 20,
		Logger:       quietLogger{},
	})
	if err != nil {
		return nil, err
	}
	return &Pebble{db: db, buf: sync.Pool{New: func() any { b := make([]byte, 0, 256); return &b }}}, nil
}

func (p *Pebble) Apply(key, value string, t uint64) {
	bp := p.buf.Get().(*[]byte)
	b := EncodeVersioned((*bp)[:0], value, t)
	// Pebble copies key and value into its memtable.
	if err := p.db.Set([]byte(key), b, pebble.NoSync); err != nil {
		panic("dyna: store write failed: " + err.Error())
	}
	*bp = b[:0]
	p.buf.Put(bp)
}

// Stage buffers a write in the pending batch (not readable until Flush).
func (p *Pebble) Stage(key, value string, t uint64) {
	if p.batch == nil {
		p.batch = p.db.NewBatch()
	}
	p.enc = EncodeVersioned(p.enc[:0], value, t)
	if err := p.batch.Set([]byte(key), p.enc, nil); err != nil { // copies
		panic("dyna: store batch failed: " + err.Error())
	}
}

// Flush commits the staged batch (no sync).
func (p *Pebble) Flush() {
	if p.batch == nil || p.batch.Empty() {
		return
	}
	if err := p.batch.Commit(pebble.NoSync); err != nil {
		panic("dyna: store write failed: " + err.Error())
	}
	p.batch.Close()
	p.batch = nil
}

func (p *Pebble) Read(key string) (VersionedValue, bool) {
	v, closer, err := p.db.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return VersionedValue{}, false
	}
	if err != nil {
		panic("dyna: store read failed: " + err.Error())
	}
	vv := DecodeVersioned(v) // copies out of Pebble's buffer
	closer.Close()
	return vv, true
}

func (p *Pebble) Close() error {
	p.Flush()
	return p.db.Close()
}

// quietLogger drops Pebble's info chatter (flushes, compactions) and keeps the rest.
type quietLogger struct{}

func (quietLogger) Infof(string, ...interface{}) {}
func (quietLogger) Errorf(format string, args ...interface{}) {
	log.Printf("pebble: "+format, args...)
}
func (quietLogger) Fatalf(format string, args ...interface{}) {
	log.Fatalf("pebble: "+format, args...)
}
