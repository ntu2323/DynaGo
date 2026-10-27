package deadline

import "testing"

func TestReplicaOwdEstimator_PercentileAndClamp(t *testing.T) {
	cfg := Config{InitialBoundUS: 500, MaxOwdUS: 1000, OwdWindowLen: 10, OwdPercentile: 50}
	e := NewReplicaOwdEstimator(cfg)
	for i := uint64(1); i <= 10; i++ {
		e.Sample(7, i*100)
	}
	if got := e.EstimateUS(7); got != 500 { // sorted[ (50*9)/100 = 4 ] = 500
		t.Fatal(got)
	}
	e.Sample(8, 99999)
	if e.EstimateUS(8) != 1000 || e.MaxEstimateUS() != 1000 {
		t.Fatal("clamp")
	}
	if e.EstimateUS(9) != 0 {
		t.Fatal("unknown client")
	}
}

func TestClientBoundTracker_ClampsAndTakesMax(t *testing.T) {
	cfg := Config{InitialBoundUS: 500, MaxOwdUS: 2000, OwdWindowLen: 10, OwdPercentile: 50}
	c := NewClientBoundTracker(cfg, 3)
	if c.BoundUS() != 500 {
		t.Fatal()
	}
	c.OnOwdSample(0, 100) // below initial: floor at initial
	if c.BoundUS() != 500 {
		t.Fatal(c.BoundUS())
	}
	c.OnOwdSample(1, 800)
	if c.BoundUS() != 800 {
		t.Fatal(c.BoundUS())
	}
	c.OnOwdSample(2, 5000) // clamped to max
	if c.BoundUS() != 2000 {
		t.Fatal(c.BoundUS())
	}
	c.OnOwdSample(9, 100) // out of range: ignored
}

func TestClientBoundTracker_CongestionFloorDecays(t *testing.T) {
	cfg := Config{InitialBoundUS: 500, MaxOwdUS: 2000, OwdWindowLen: 10, OwdPercentile: 50}
	c := NewClientBoundTracker(cfg, 3)
	c.OnObservedHold(1000, 4000) // held 3000 > bound
	if c.BoundUS() != 3000 {
		t.Fatal(c.BoundUS())
	}
	for i := 0; i < 200; i++ {
		c.OnObservedHold(1000, 1100)
	}
	if c.BoundUS() != 500 {
		t.Fatal("floor must decay", c.BoundUS())
	}
}
