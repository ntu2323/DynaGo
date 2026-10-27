// dyna-client is the benchmark client: -j DYNA clients (one UDP socket each) issue
// single-key writes (and optionally reads), closed loop or open loop.
//
// It accepts the C++ bench/client flags and prints the same log lines (interval
// "Completed N requests at T ms", the finish line, "Median latency is X ns", ...), so
// run_cluster_bench.py / aggregate_bench_results.py drive it unchanged:
//
//	dyna-client -c config -j 4 -e 10 -w 2                       # closed loop, 10 s
//	dyna-client -c config -j 8 -e 30 -w 5 -O -G 100000 -i 1000   # open loop, 100k ops/s
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"dynago/internal/client"
	"dynago/internal/config"
	"dynago/internal/transport"
	"dynago/internal/types"
)

type worker struct {
	cl      *client.Client
	tr      *transport.UDP
	lat     []time.Duration
	issued  atomic.Int64
	issuedM atomic.Int64 // issued while measuring (the C++ finish line's submitted=)
	done    atomic.Int64 // measured completions
	slow    atomic.Int64
	mu      sync.Mutex
}

// loadTraceKeys reads the bench trace (C++ trace.micro: "{op} {key} " repeated).
func loadTraceKeys(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<30)
	sc.Split(bufio.ScanWords)
	var keys []string
	for i := 0; sc.Scan(); i++ {
		if i%2 == 1 {
			keys = append(keys, sc.Text())
		}
	}
	return keys, sc.Err()
}

func fmtNS(ns int64) string { return time.Duration(ns).String() }

func main() {
	cfgPath := flag.String("c", "", "cluster config file")
	flag.String("s", "", "consensus config (accepted for bench-script compatibility; unused)")
	flag.String("m", "dyna", "protocol (accepted for script compatibility)")
	traceFile := flag.String("k", "", "key trace (C++ trace.micro); default: -keys generated keys")
	threads := flag.Int("j", 1, "number of clients (goroutines, one socket each)")
	dur := flag.Int("e", 10, "measured duration, seconds")
	warm := flag.Int("w", 1, "warmup, seconds (not measured; open loop ramps 0→100% over it)")
	numReq := flag.Int("n", 0, "closed loop: stop each client after n requests (0 = duration)")
	machine := flag.Uint64("t", 0, "machine / client id: client ids are t<<20 + thread (0 = random)")
	intervalMS := flag.Int("i", 0, "print 'Completed N requests at T ms' every i ms (0 = off)")
	latFile := flag.String("l", "", "write measured latencies (ns, one per line) here")
	openLoop := flag.Bool("O", false, "open loop (with -G or -R)")
	offered := flag.Float64("G", 0, "open loop: offered ops/s for this process")
	poisson := flag.Int("R", 0, "open loop: requests per 10 ms tick for this process (C++ -R)")
	maxInflight := flag.Int("F", 0, "open loop: cap on outstanding requests (0 = none)")
	rate := flag.Float64("rate", 0, "open loop: offered ops/s (alias of -O -G)")
	nkeys := flag.Int("keys", 1000, "key space size when no -k trace")
	vsize := flag.Int("value-size", 100, "value bytes (C++ bench: 100)")
	readFrac := flag.Float64("reads", 0, "fraction of reads")
	cpuProf := flag.String("cpuprofile", "", "write a CPU profile to this file")
	flag.Parse()
	log.SetFlags(log.Lmicroseconds)
	// Fewer collections (see dyna-replica). GOGC in the environment overrides this.
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(400)
	}
	if a := os.Getenv("DYNA_PPROF"); a != "" { // e.g. 127.0.0.1:6061
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
	if *rate > 0 {
		*openLoop, *offered = true, *rate
	}
	if *openLoop && *offered <= 0 && *poisson > 0 {
		*offered = float64(*poisson) * 100 // per 10 ms tick
	}
	if *openLoop && *offered <= 0 {
		log.Fatal("open loop needs -G (ops/s) or -R")
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	addrs, err := cfg.Resolve()
	if err != nil {
		log.Fatal(err)
	}
	idBase := *machine << 20
	if idBase == 0 {
		idBase = uint64(rand.Int63n(1<<40)) << 20
	}
	var keys []string
	if *traceFile != "" {
		if keys, err = loadTraceKeys(*traceFile); err != nil || len(keys) == 0 {
			log.Fatalf("trace %s: %v (%d keys)", *traceFile, err, len(keys))
		}
	} else {
		for i := 0; i < *nkeys; i++ {
			keys = append(keys, fmt.Sprintf("user%020d", i+1))
		}
	}
	val := strings.Repeat("x", *vsize)

	ws := make([]*worker, *threads)
	for i := range ws {
		tr, err := transport.ListenUDP("0.0.0.0:0", -1, addrs)
		if err != nil {
			log.Fatal(err)
		}
		cl := client.New(client.Options{ID: idBase + uint64(i) + 1, F: cfg.F, Transport: tr})
		go tr.Run(cl)
		cl.Start()
		ws[i] = &worker{cl: cl, tr: tr}
	}

	start := time.Now()
	measureFrom := start.Add(time.Duration(*warm) * time.Second)
	end := measureFrom.Add(time.Duration(*dur) * time.Second)
	var measuring atomic.Bool
	var completedAll, inflight atomic.Int64 // all completions (interval lines), outstanding

	issue := func(w *worker, rng *rand.Rand, onDone func()) {
		key := keys[rng.Intn(len(keys))]
		t0 := time.Now()
		w.issued.Add(1)
		inflight.Add(1)
		measured := measuring.Load()
		if measured {
			w.issuedM.Add(1)
		}
		cb := func(r client.Result) {
			inflight.Add(-1)
			completedAll.Add(1)
			if measured && measuring.Load() {
				w.done.Add(1)
				if r.Slow {
					w.slow.Add(1)
				}
				w.mu.Lock()
				w.lat = append(w.lat, time.Since(t0))
				w.mu.Unlock()
			}
			if onDone != nil {
				onDone()
			}
		}
		if *readFrac > 0 && rng.Float64() < *readFrac {
			w.cl.Read([]string{key}, cb)
		} else {
			w.cl.Write([]types.KV{{Key: key, Value: val}}, cb)
		}
	}

	var wg sync.WaitGroup
	for i, w := range ws {
		wg.Add(1)
		go func(i int, w *worker) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(idBase) + int64(i)))
			if !*openLoop {
				// Closed loop: one outstanding request per client.
				next := make(chan struct{}, 1)
				for n := 0; time.Now().Before(end) && (*numReq == 0 || n < *numReq); n++ {
					issue(w, rng, func() { next <- struct{}{} })
					select {
					case <-next:
					case <-time.After(time.Until(end) + time.Second):
						return
					}
				}
				return
			}
			// Open loop, smoothed: per client offered/j, issued on a 1 ms tick with a random
			// phase; ramped 0→100% over the warmup.
			per := *offered / float64(*threads)
			time.Sleep(time.Duration(rng.Intn(1000)) * time.Microsecond)
			tk := time.NewTicker(time.Millisecond)
			defer tk.Stop()
			owed := 0.0
			last := time.Now()
			for now := range tk.C {
				if !now.Before(end) {
					return
				}
				frac := 1.0
				if now.Before(measureFrom) && *warm > 0 {
					frac = float64(now.Sub(start)) / float64(measureFrom.Sub(start))
				}
				owed += per * frac * now.Sub(last).Seconds()
				last = now
				for ; owed >= 1; owed-- {
					if *maxInflight > 0 && inflight.Load() >= int64(*maxInflight) {
						owed = 0 // C++ -F: skip, don't queue
						break
					}
					issue(w, rng, nil)
				}
			}
		}(i, w)
	}
	submitted := func() (n int64) {
		for _, w := range ws {
			n += w.issued.Load()
		}
		return n
	}
	stopIv := make(chan struct{})
	if *intervalMS > 0 {
		go func() {
			tk := time.NewTicker(time.Duration(*intervalMS) * time.Millisecond)
			defer tk.Stop()
			var last int64
			for {
				select {
				case <-stopIv:
					return
				case now := <-tk.C:
					c := completedAll.Load()
					log.Printf("Completed %d requests at %d ms inflight=%d submitted=%d", c-last,
						now.UnixMilli()/10*10, inflight.Load(), submitted())
					last = c
				}
			}
		}()
	}
	time.Sleep(time.Until(measureFrom))
	measuring.Store(true)
	log.Printf("Completed warmup period of %d seconds with %d completions (%d submitted)",
		*warm, completedAll.Load(), submitted())
	time.Sleep(time.Until(end))
	measuring.Store(false)
	measured := time.Since(measureFrom)
	wg.Wait()
	time.Sleep(200 * time.Millisecond)
	close(stopIv)

	var lat []time.Duration
	var done, slow, issued, issuedM int64
	var st client.Stats
	for _, w := range ws {
		w.mu.Lock()
		lat = append(lat, w.lat...)
		w.mu.Unlock()
		done += w.done.Load()
		slow += w.slow.Load()
		issued += w.issued.Load()
		issuedM += w.issuedM.Load()
		s := w.cl.Stats()
		st.Fast += s.Fast
		st.SlowDisagree += s.SlowDisagree
		st.SlowTimeout += s.SlowTimeout
		st.ResendWrite += s.ResendWrite
		st.RetryRead += s.RetryRead
		st.ResendSlowCommit += s.ResendSlowCommit
		w.cl.Stop()
		w.tr.Close()
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[min(len(lat)-1, int(p*float64(len(lat))))]
	}
	// C++ bench lines (aggregate_bench_results.py parses these).
	log.Printf("Completed %d requests in %.6f seconds (submitted=%d)", done, measured.Seconds(), issuedM)
	if len(lat) == 0 {
		log.Printf("No latency samples recorded.")
	} else {
		var sum time.Duration
		for _, l := range lat {
			sum += l
		}
		for _, x := range []struct {
			name string
			ns   int64
		}{
			{"Median", int64(lat[len(lat)/2])},
			{"Average", int64(sum) / int64(len(lat))},
			{"90th percentile", int64(lat[len(lat)*90/100])},
			{"95th percentile", int64(lat[len(lat)*95/100])},
			{"99th percentile", int64(lat[len(lat)*99/100])},
		} {
			log.Printf("%s latency is %d ns (%s)", x.name, x.ns, fmtNS(x.ns))
		}
	}
	if *latFile != "" {
		if f, err := os.Create(*latFile); err == nil {
			bw := bufio.NewWriter(f)
			for _, l := range lat {
				fmt.Fprintln(bw, int64(l))
			}
			bw.Flush()
			f.Close()
		}
	}
	// Human-readable summary (scripts/smoke.sh).
	fmt.Printf("Completed %d requests in %ds (%.0f ops/s); issued %d total\n", done, *dur,
		float64(done)/measured.Seconds(), issued)
	fmt.Printf("latency median %.3f ms  p90 %.3f ms  p99 %.3f ms  max %.3f ms\n",
		pct(0.5).Seconds()*1e3, pct(0.9).Seconds()*1e3, pct(0.99).Seconds()*1e3, pct(1).Seconds()*1e3)
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	cpu := time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	fmt.Printf("client cpu: %.2f cores avg over %.1fs\n", cpu.Seconds()/time.Since(start).Seconds(),
		time.Since(start).Seconds())
	over := 0
	for i := len(lat) - 1; i >= 0 && lat[i] > 25*time.Millisecond; i-- {
		over++
	}
	fmt.Printf("client stats: fast=%d slow[disagree=%d timeout=%d] resendW=%d resendSlowCommit=%d retryR=%d (measured slow=%d, >25ms=%d)\n",
		st.Fast, st.SlowDisagree, st.SlowTimeout, st.ResendWrite, st.ResendSlowCommit, st.RetryRead, slow, over)
}
