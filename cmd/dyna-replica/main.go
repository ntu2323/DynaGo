// dyna-replica runs one DYNA replica over UDP.
//
//	dyna-replica -c config -i 0
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"syscall"

	"net/http"
	_ "net/http/pprof"

	"dynago/internal/config"
	"dynago/internal/deadline"
	"dynago/internal/orderer"
	"dynago/internal/replica"
	"dynago/internal/store"
	"dynago/internal/transport"
)

func main() {
	cfgPath := flag.String("c", "", "cluster config file")
	idx := flag.Int("i", -1, "replica index")
	flag.String("m", "dyna", "protocol (only dyna; accepted for script compatibility)")
	cpuProf := flag.String("cpuprofile", "", "write a CPU profile to this file until exit")
	memProf := flag.String("memprofile", "", "write an allocation profile to this file at exit")
	flag.Parse()
	// Fewer collections: the heap is small next to a benchmark node's RAM, and every GC
	// cycle's stop-the-world phases and assist work land in the deadline budget.
	// GOGC / GOMEMLIMIT in the environment override this.
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(400)
	}
	log.SetFlags(log.Lmicroseconds)
	if a := os.Getenv("DYNA_PPROF"); a != "" { // e.g. 127.0.0.1:6060
		if os.Getenv("DYNA_PPROF_LOCKS") == "1" { // mutex + block profiles (some overhead)
			runtime.SetMutexProfileFraction(100)
			runtime.SetBlockProfileRate(100000) // sample blocking events >= ~100 µs
		}
		go func() { log.Println(http.ListenAndServe(a, nil)) }()
	}
	if *cpuProf != "" {
		f, err := os.Create(*cpuProf)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	if *idx < 0 || *idx >= cfg.N() {
		log.Fatalf("bad replica index %d (n=%d)", *idx, cfg.N())
	}
	addrs, err := cfg.Resolve()
	if err != nil {
		log.Fatal(err)
	}
	// One socket (and receive goroutine) per DYNA_RX_THREADS; each sender goroutine also
	// gets its own socket to write on (socket 0 carries inline sends).
	rx := int(deadline.EnvU64("DYNA_RX_THREADS", 1))
	senders := int(deadline.EnvU64("DYNA_SEND_THREADS", 1))
	tr, err := transport.ListenUDPOpts(cfg.Replicas[*idx], *idx, addrs, transport.UDPOptions{
		Sockets: max(rx, senders+1), RxBatch: int(deadline.EnvU64("DYNA_RX_BATCH", 64))})
	if err != nil {
		log.Fatal(err)
	}
	st, closeStore := openStore(*idx)
	r := replica.New(replica.Options{Index: *idx, F: cfg.F, Transport: tr, Store: st})
	log.Printf("DYNA-go r%d up: n=%d f=%d lanes=%d sockets=%d senders=%d addr=%s", *idx, cfg.N(),
		cfg.F, orderer.LanesFromEnv(), tr.Sockets(), senders, tr.Self())
	go tr.Run(r)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	tr.Close()
	r.Stop()
	closeStore()
	if *memProf != "" {
		if f, err := os.Create(*memProf); err == nil {
			_ = pprof.Lookup("allocs").WriteTo(f, 0)
			f.Close()
		}
	}
	o := r.Orderer()
	log.Printf("r%d final: executed=%d speclog=%d early=%d late=%d", *idx, o.ExecutedCount(),
		o.SpecLogSize(), o.EarlySize(), o.LateSize())
}

// openStore picks the executed-write store: DYNA_STORE=mem (default) | pebble, or the
// bench scripts' IONIA_DB_MODE (memory | splinter | pebble; anything on disk → Pebble).
// The disk store lives in $DYNA_DB_DIR/dyna-pebble-<idx> and is wiped at startup: it is
// not trusted across a restart (no fsync); rejoin catch-up refills it.
func openStore(idx int) (store.Store, func()) {
	mode := os.Getenv("DYNA_STORE")
	if mode == "" {
		switch os.Getenv("IONIA_DB_MODE") {
		case "", "memory", "mem":
			mode = "mem"
		default:
			mode = "pebble"
		}
	}
	switch mode {
	case "mem", "memory":
		return store.NewMem(), func() {}
	case "pebble":
		dir := os.Getenv("DYNA_DB_DIR")
		if dir == "" {
			dir = "."
		}
		path := filepath.Join(dir, fmt.Sprintf("dyna-pebble-%d", idx))
		if err := os.RemoveAll(path); err != nil {
			log.Fatal(err)
		}
		p, err := store.OpenPebble(path, int(deadline.EnvU64("DYNA_DB_CACHE_MB", 256)))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("store: pebble at %s", path)
		return p, func() { p.Close() }
	}
	log.Fatalf("unknown DYNA_STORE %q (mem | pebble)", mode)
	return nil, nil
}
