// Package config parses the cluster config format shared with the C++ harness:
//
//	f 1
//	replica 10.10.1.1:13000
//	replica 10.10.1.3:13000
//	replica 10.10.1.4:13000
package config

import (
	"bufio"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	F        int
	Replicas []string // host:port as written
}

func (c *Config) N() int { return len(c.Replicas) }

func Parse(text string) (*Config, error) {
	c := &Config{F: -1}
	sc := bufio.NewScanner(strings.NewReader(text))
	for line := 1; sc.Scan(); line++ {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		switch fields[0] {
		case "f":
			if len(fields) != 2 {
				return nil, fmt.Errorf("config line %d: want 'f <n>'", line)
			}
			f, err := strconv.Atoi(fields[1])
			if err != nil {
				return nil, fmt.Errorf("config line %d: %v", line, err)
			}
			c.F = f
		case "replica":
			if len(fields) != 2 {
				return nil, fmt.Errorf("config line %d: want 'replica host:port'", line)
			}
			c.Replicas = append(c.Replicas, fields[1])
		default:
			// Other C++ directives (e.g. multicast) are irrelevant here.
		}
	}
	if c.F < 0 {
		c.F = (len(c.Replicas) - 1) / 2
	}
	if len(c.Replicas) == 0 || len(c.Replicas) < 2*c.F+1 {
		return nil, fmt.Errorf("config: %d replicas for f=%d", len(c.Replicas), c.F)
	}
	if len(c.Replicas) > 64 {
		return nil, fmt.Errorf("config: at most 64 replicas")
	}
	return c, nil
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(string(b))
}

// Resolve turns every replica address into an AddrPort (IPv4 preferred).
func (c *Config) Resolve() ([]netip.AddrPort, error) {
	out := make([]netip.AddrPort, len(c.Replicas))
	for i, r := range c.Replicas {
		ua, err := net.ResolveUDPAddr("udp4", r)
		if err != nil {
			return nil, fmt.Errorf("replica %d (%s): %v", i, r, err)
		}
		ap := ua.AddrPort()
		out[i] = netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	}
	return out, nil
}
