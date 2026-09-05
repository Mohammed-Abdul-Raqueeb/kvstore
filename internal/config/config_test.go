package config

import (
	"flag"
	"io"
	"testing"
)

func TestParseBytes(t *testing.T) {
	cases := map[string]int64{
		"0":      0,
		"512":    512,
		"1KB":    1024,
		"1kib":   1024,
		"64MB":   64 << 20,
		"16MiB":  16 << 20,
		"2gb":    2 << 30,
		"1.5MB":  1572864,
		" 8 mb ": 8 << 20,
	}
	for in, want := range cases {
		got, err := ParseBytes(in)
		if err != nil {
			t.Fatalf("ParseBytes(%q): %v", in, err)
		}
		if got != want {
			t.Fatalf("ParseBytes(%q) = %d, want %d", in, got, want)
		}
	}
	for _, bad := range []string{"", "MB", "12xb", "abc"} {
		if _, err := ParseBytes(bad); err == nil {
			t.Fatalf("ParseBytes(%q) should have failed", bad)
		}
	}
}

func TestDefaultsValidate(t *testing.T) {
	c := Default()
	c.Normalise()
	if err := c.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	if c.Shards&(c.Shards-1) != 0 {
		t.Fatalf("default shard count %d is not a power of two", c.Shards)
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	cases := map[string]func(*Config){
		"non-pow2 shards":  func(c *Config) { c.Shards = 6 },
		"unknown engine":   func(c *Config) { c.Engine = "magic" },
		"unknown fsync":    func(c *Config) { c.Fsync = "sometimes" },
		"unknown policy":   func(c *Config) { c.Policy = "random" },
		"unknown expiry":   func(c *Config) { c.Expiry = "psychic" },
		"replica no addr":  func(c *Config) { c.Role = RoleReplica; c.PrimaryAddr = "" },
		"bad sample k":     func(c *Config) { c.EvictSampleK = 0 },
		"bad low water":    func(c *Config) { c.LowWaterRatio = 1.5 },
		"tiny segment":     func(c *Config) { c.SegmentSize = 10 },
		"empty data dir":   func(c *Config) { c.DataDir = "" },
		"huge value limit": func(c *Config) { c.MaxValueLen = 1 << 30 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := Default()
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestFlagsAndEnvPrecedence(t *testing.T) {
	t.Setenv("KV_ADDR", "0.0.0.0:9999")
	t.Setenv("KV_MAX_MEMORY", "128MB")
	t.Setenv("KV_SHARDS", "8")

	c := Default()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	c.RegisterFlags(fs)
	// --shards is given explicitly, so the env var for it must lose.
	if err := fs.Parse([]string{"--shards=32"}); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyEnv(fs); err != nil {
		t.Fatal(err)
	}

	if c.Addr != "0.0.0.0:9999" {
		t.Fatalf("env did not apply to addr: %q", c.Addr)
	}
	if c.MaxMemory != 128<<20 {
		t.Fatalf("env size parsing failed: %d", c.MaxMemory)
	}
	if c.Shards != 32 {
		t.Fatalf("explicit flag should beat env: got %d, want 32", c.Shards)
	}
}

func TestLowWaterMark(t *testing.T) {
	c := Default()
	c.MaxMemory = 1000
	c.LowWaterRatio = 0.95
	if got := c.LowWaterMark(); got != 950 {
		t.Fatalf("LowWaterMark = %d, want 950", got)
	}
	c.MaxMemory = 0
	if got := c.LowWaterMark(); got != 0 {
		t.Fatalf("unlimited memory should give low-water 0, got %d", got)
	}
}

func TestReplicaofImpliesReplicaRole(t *testing.T) {
	c := Default()
	c.PrimaryAddr = "127.0.0.1:7379"
	c.Normalise()
	if c.Role != RoleReplica {
		t.Fatalf("role = %q, want replica", c.Role)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}
