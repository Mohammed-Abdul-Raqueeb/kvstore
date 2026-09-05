package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeSnapshot(t *testing.T, dir string, lsn uint64, n int) string {
	t.Helper()
	w, err := Create(dir, lsn, 1700000000000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := w.Add(Entry{
			Key:        []byte(fmt.Sprintf("key%05d", i)),
			Value:      []byte(fmt.Sprintf("value%05d", i)),
			ExpireAtMs: uint64(i % 3),
		}); err != nil {
			t.Fatal(err)
		}
	}
	path, err := w.Commit()
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const n = 5000
	path := writeSnapshot(t, dir, 12345, n)

	seen := 0
	hdr, err := Load(path, func(e Entry) error {
		if got, want := string(e.Key), fmt.Sprintf("key%05d", seen); got != want {
			return fmt.Errorf("entry %d: key %q, want %q", seen, got, want)
		}
		if got, want := string(e.Value), fmt.Sprintf("value%05d", seen); got != want {
			return fmt.Errorf("entry %d: value %q, want %q", seen, got, want)
		}
		if e.ExpireAtMs != uint64(seen%3) {
			return fmt.Errorf("entry %d: expireAt %d", seen, e.ExpireAtMs)
		}
		seen++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != n {
		t.Fatalf("loaded %d entries, want %d", seen, n)
	}
	if hdr.LastIncludedLSN != 12345 {
		t.Fatalf("LastIncludedLSN = %d", hdr.LastIncludedLSN)
	}
	if hdr.EntryCount != n {
		t.Fatalf("EntryCount = %d, want %d", hdr.EntryCount, n)
	}
}

func TestSnapshotEmptyKeyspace(t *testing.T) {
	dir := t.TempDir()
	path := writeSnapshot(t, dir, 7, 0)
	count := 0
	hdr, err := Load(path, func(Entry) error { count++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || hdr.EntryCount != 0 {
		t.Fatalf("empty snapshot loaded %d entries", count)
	}
}

func TestSnapshotHandlesEmptyAndBinaryKeys(t *testing.T) {
	dir := t.TempDir()
	w, err := Create(dir, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		{Key: []byte{}, Value: []byte{}},
		{Key: []byte{0x00}, Value: []byte{0xFF, 0x00}},
		{Key: []byte("normal"), Value: bytes.Repeat([]byte("v"), 100000)},
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	path, err := w.Commit()
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	if _, err := Load(path, func(e Entry) error {
		if !bytes.Equal(e.Key, entries[i].Key) || !bytes.Equal(e.Value, entries[i].Value) {
			return fmt.Errorf("entry %d mismatch", i)
		}
		i++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if i != len(entries) {
		t.Fatalf("loaded %d of %d entries", i, len(entries))
	}
}

func TestTmpFileIsRenamedNotWrittenInPlace(t *testing.T) {
	dir := t.TempDir()
	w, err := Create(dir, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(Entry{Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}

	// Before Commit, the final name must not exist and a .tmp must.
	final := filepath.Join(dir, Name(100))
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatal("snapshot appeared under its final name before Commit")
	}
	if _, err := os.Stat(final + ".tmp"); err != nil {
		t.Fatalf("no .tmp file during writing: %v", err)
	}

	if _, err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(final); err != nil {
		t.Fatalf("snapshot missing after Commit: %v", err)
	}
	if _, err := os.Stat(final + ".tmp"); !os.IsNotExist(err) {
		t.Fatal(".tmp file survived Commit")
	}
}

func TestAbortLeavesNothingBehind(t *testing.T) {
	dir := t.TempDir()
	w, err := Create(dir, 55, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.Add(Entry{Key: []byte("k"), Value: []byte("v")})
	w.Abort()

	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("Abort left %d files behind", len(entries))
	}
}

func TestCorruptSnapshotIsRejected(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(t *testing.T, path string)
		wantErr error
	}{
		{"truncated body", func(t *testing.T, p string) {
			fi, _ := os.Stat(p)
			os.Truncate(p, fi.Size()/2)
		}, nil},
		{"missing footer", func(t *testing.T, p string) {
			fi, _ := os.Stat(p)
			os.Truncate(p, fi.Size()-FooterLen)
		}, nil},
		{"bit flip in body", func(t *testing.T, p string) {
			d, _ := os.ReadFile(p)
			d[HeaderLen+10] ^= 0x40
			os.WriteFile(p, d, 0o644)
		}, ErrBadCRC},
		{"bit flip in header", func(t *testing.T, p string) {
			d, _ := os.ReadFile(p)
			d[20] ^= 0x01 // last_included_lsn
			os.WriteFile(p, d, 0o644)
		}, ErrBadCRC},
		{"bad magic", func(t *testing.T, p string) {
			d, _ := os.ReadFile(p)
			d[0] = 'X'
			os.WriteFile(p, d, 0o644)
		}, ErrBadCRC},
		{"empty file", func(t *testing.T, p string) {
			os.WriteFile(p, nil, 0o644)
		}, ErrIncomplete},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeSnapshot(t, dir, 1, 200)
			tc.mutate(t, path)

			applied := 0
			_, err := Load(path, func(Entry) error { applied++; return nil })
			if err == nil {
				t.Fatal("corrupt snapshot loaded successfully")
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
			// Nothing must have been handed to the caller: verification
			// happens before any entry is emitted.
			if applied != 0 {
				t.Fatalf("%d entries were emitted from a corrupt snapshot", applied)
			}
		})
	}
}

func TestLoadNewestValidFallsBackToOlder(t *testing.T) {
	dir := t.TempDir()
	writeSnapshot(t, dir, 100, 50)
	newer := writeSnapshot(t, dir, 200, 50)

	// Corrupt the newer one, simulating a crash that somehow left a bad
	// file under a final name.
	d, _ := os.ReadFile(newer)
	d[HeaderLen+5] ^= 0xFF
	os.WriteFile(newer, d, 0o644)

	count := 0
	hdr, path, err := LoadNewestValid(dir, func(Entry) error { count++; return nil })
	if err != nil {
		t.Fatalf("should have fallen back to the older snapshot: %v", err)
	}
	if hdr.LastIncludedLSN != 100 {
		t.Fatalf("loaded snapshot at LSN %d, want the older one at 100", hdr.LastIncludedLSN)
	}
	if count != 50 {
		t.Fatalf("loaded %d entries", count)
	}
	t.Logf("fell back to %s", path)
}

func TestLoadNewestValidPrefersNewest(t *testing.T) {
	dir := t.TempDir()
	writeSnapshot(t, dir, 100, 10)
	writeSnapshot(t, dir, 300, 20)
	writeSnapshot(t, dir, 200, 15)

	count := 0
	hdr, _, err := LoadNewestValid(dir, func(Entry) error { count++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if hdr.LastIncludedLSN != 300 || count != 20 {
		t.Fatalf("loaded LSN %d with %d entries, want 300/20", hdr.LastIncludedLSN, count)
	}
}

func TestLoadNewestValidNoSnapshots(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := LoadNewestValid(dir, func(Entry) error { return nil }); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("got %v, want ErrNoSnapshot", err)
	}
}

func TestPrune(t *testing.T) {
	dir := t.TempDir()
	for _, lsn := range []uint64{100, 200, 300, 400} {
		writeSnapshot(t, dir, lsn, 5)
	}
	os.WriteFile(filepath.Join(dir, "0000000000000999.snap.tmp"), []byte("junk"), 0o644)

	removed, err := Prune(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("pruned %d snapshots, want 2", removed)
	}
	snaps, _ := List(dir)
	if len(snaps) != 2 || snaps[0].LastIncludedLSN != 400 || snaps[1].LastIncludedLSN != 300 {
		t.Fatalf("wrong snapshots kept: %+v", snaps)
	}
	if _, err := os.Stat(filepath.Join(dir, "0000000000000999.snap.tmp")); !os.IsNotExist(err) {
		t.Fatal("abandoned .tmp file was not cleaned up")
	}
}

func TestListIsNewestFirst(t *testing.T) {
	dir := t.TempDir()
	for _, lsn := range []uint64{5, 500, 50} {
		writeSnapshot(t, dir, lsn, 1)
	}
	snaps, err := List(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{500, 50, 5}
	for i, s := range snaps {
		if s.LastIncludedLSN != want[i] {
			t.Fatalf("snapshot %d has LSN %d, want %d", i, s.LastIncludedLSN, want[i])
		}
	}
}
