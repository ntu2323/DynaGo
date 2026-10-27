package config

import "testing"

func TestParse(t *testing.T) {
	c, err := Parse("f 1\nreplica localhost:13000\nreplica localhost:13001\n\nreplica 127.0.0.1:13002\n")
	if err != nil || c.F != 1 || c.N() != 3 {
		t.Fatal(c, err)
	}
	a, err := c.Resolve()
	if err != nil || a[2].Port() != 13002 {
		t.Fatal(a, err)
	}
	if _, err := Parse("f 1\nreplica a:1\n"); err == nil {
		t.Fatal("too few replicas accepted")
	}
}
