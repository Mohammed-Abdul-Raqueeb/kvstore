package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/raqueeb/kvstore/internal/config"
)

func testOptions(dir string) Options {
	return Options{
		Dir:             dir,
		Fsync:           config.FsyncAlways,
		SegmentSize:     1 << 20,
		GroupCommitMax:  64,
		GroupCommitWait: time.Millisecond,
		QueueDepth:      1024,
		StartLSN:        1,
	}
}

func TestRecordRoundTrip(t *testing.T) {
	cases := []Record{
		{LSN: 1, CreatedAtMs: 12345, Type: RecSet, Key: []byte("k"), Value: []byte("v")},
		{LSN: 2, CreatedAtMs: 0, Type: RecDelete, Key: []byte("gone")},
		{LSN: 3, Type: RecExpire, Key: []byte("k"), ExpireAtMs: 999999},
		{LSN: 4, Type: RecSet, Key: nil, Value: nil},
		{LSN: 5, Type: RecSet, Key: bytes.Repeat([]byte("k"), maxKeyLen), Value: []byte("v")},
		{LSN: 6, Type: RecSet, Key: []byte("big"), Value: bytes.Repeat([]byte("v"), 1<<20)},
		{LSN: 7, Type: RecSet, Key: []byte{0x00, 0xFF, '\n'}, Value: []byte{0x00}},
		{LSN: ^uint64(0), Type: RecSet, Key: []byte("max-lsn"), Value: []byte("v"), Flags: 0xAB},
	}
	for i, want := range cases {
		buf := AppendRecord(nil, want)
		if len(buf) != want.EncodedLen() {
			t.Fatalf("case %d: encoded %d bytes, EncodedLen says %d", i, len(buf), want.EncodedLen())
		}
		got, n, err := DecodeRecord(buf, int64(len(buf)))
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if n != len(buf) {
			t.Fatalf("case %d: consumed %d of %d bytes", i, n, len(buf))
		}
		if got.LSN != want.LSN || got.Type != want.Type || got.Flags != want.Flags ||
			got.CreatedAtMs != want.CreatedAtMs || got.ExpireAtMs != want.ExpireAtMs ||
			!bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("case %d mismatch:\n got %+v\nwant %+v", i, got, want)
		}
	}
}

func TestRecordsAreSelfDelimiting(t *testing.T) {
	var buf []byte
	for i := 1; i <= 100; i++ {
		buf = AppendRecord(buf, Record{
			LSN: uint64(i), Type: RecSet,
			Key:   []byte(fmt.Sprintf("k%d", i)),
			Value: bytes.Repeat([]byte("v"), i),
		})
	}
	offset, count := 0, 0
	for offset < len(buf) {
		r, n, err := DecodeRecord(buf[offset:], int64(len(buf)-offset))
		if err != nil {
			t.Fatalf("record %d at offset %d: %v", count, offset, err)
		}
		count++
		if r.LSN != uint64(count) {
			t.Fatalf("record %d has LSN %d", count, r.LSN)
		}
		offset += n
	}
	if count != 100 {
		t.Fatalf("decoded %d records, want 100", count)
	}
}

// TestCRCDetectsSingleBitFlips is the test DESIGN.md §11 asks for by name:
// flip one bit at every byte position and assert the corruption is caught.
func TestCRCDetectsSingleBitFlips(t *testing.T) {
	orig := AppendRecord(nil, Record{
		LSN: 42, CreatedAtMs: 1700000000000, Type: RecSet,
		Key: []byte("some-key"), Value: []byte("some-value-payload"), ExpireAtMs: 123456,
	})

	undetected := 0
	for bytePos := 0; bytePos < len(orig); bytePos++ {
		for bit := 0; bit < 8; bit++ {
			corrupt := append([]byte(nil), orig...)
			corrupt[bytePos] ^= 1 << bit

			_, _, err := DecodeRecord(corrupt, int64(len(corrupt)))
			if err != nil {
				continue // detected, as required
			}
			// A flip in the length field can make the record *look* shorter
			// and still checksum, only because we handed DecodeRecord a
			// buffer whose extra bytes it ignores. Confirm that such a
			// record is nonetheless rejected on a real scan, where the
			// following bytes have to parse as another record.
			undetected++
			t.Errorf("bit flip at byte %d bit %d was not detected", bytePos, bit)
		}
	}
	if undetected > 0 {
		t.Fatalf("%d single-bit flips went undetected", undetected)
	}
}

func TestDecodeRejectsHostileLengths(t *testing.T) {
	valid := AppendRecord(nil, Record{LSN: 1, Type: RecSet, Key: []byte("k"), Value: []byte("v")})

	t.Run("length beyond MaxRecordLen", func(t *testing.T) {
		b := append([]byte(nil), valid...)
		binary.LittleEndian.PutUint32(b[4:8], MaxRecordLen+1)
		_, _, err := DecodeRecord(b, int64(len(b)))
		if !errors.Is(err, ErrLengthTooBig) {
			t.Fatalf("got %v, want ErrLengthTooBig", err)
		}
	})

	t.Run("4GiB length", func(t *testing.T) {
		b := append([]byte(nil), valid...)
		binary.LittleEndian.PutUint32(b[4:8], 0xFFFFFFFF)
		_, _, err := DecodeRecord(b, int64(len(b)))
		if !errors.Is(err, ErrLengthTooBig) {
			t.Fatalf("got %v, want ErrLengthTooBig", err)
		}
	})

	t.Run("length past end of file", func(t *testing.T) {
		b := append([]byte(nil), valid...)
		binary.LittleEndian.PutUint32(b[4:8], 100000)
		_, _, err := DecodeRecord(b, int64(len(b)))
		if !errors.Is(err, ErrTornTail) {
			t.Fatalf("got %v, want ErrTornTail", err)
		}
	})

	t.Run("truncated buffer", func(t *testing.T) {
		for cut := 0; cut < len(valid); cut++ {
			if _, _, err := DecodeRecord(valid[:cut], int64(len(valid))); err == nil {
				t.Fatalf("truncation at %d accepted", cut)
			}
		}
	})

	t.Run("unknown record type", func(t *testing.T) {
		r := Record{LSN: 1, Type: RecordType(99), Key: []byte("k")}
		b := AppendRecord(nil, r)
		_, _, err := DecodeRecord(b, int64(len(b)))
		if !errors.Is(err, ErrBadRecordType) {
			t.Fatalf("got %v, want ErrBadRecordType", err)
		}
	})
}

func TestWriterAppendAndScan(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}

	const n = 1000
	for i := 0; i < n; i++ {
		lsn, err := w.SubmitWait(Record{
			Type:  RecSet,
			Key:   []byte(fmt.Sprintf("key%04d", i)),
			Value: []byte(fmt.Sprintf("val%04d", i)),
		})
		if err != nil {
			t.Fatal(err)
		}
		if lsn != uint64(i+1) {
			t.Fatalf("record %d got LSN %d", i, lsn)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var seen int
	res, err := Replay(dir, 0, false, func(r Record) error {
		if got, want := string(r.Key), fmt.Sprintf("key%04d", seen); got != want {
			return fmt.Errorf("record %d: key %q, want %q", seen, got, want)
		}
		seen++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != n || res.Applied != n {
		t.Fatalf("replayed %d records (Applied=%d), want %d", seen, res.Applied, n)
	}
	if res.LastLSN != n {
		t.Fatalf("LastLSN = %d, want %d", res.LastLSN, n)
	}
	if res.Truncated {
		t.Fatalf("clean shutdown produced a truncation: %s", res.TruncateReason)
	}
}

func TestGroupCommitAmortisesFsync(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(dir)
	o.Fsync = config.FsyncAlways
	o.GroupCommitMax = 512
	o.GroupCommitWait = 5 * time.Millisecond
	w, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const writers, perWriter = 32, 100
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				if _, err := w.SubmitWait(Record{
					Type:  RecSet,
					Key:   []byte(fmt.Sprintf("k%d-%d", i, j)),
					Value: []byte("v"),
				}); err != nil {
					t.Error(err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	st := w.Stats()
	total := uint64(writers * perWriter)
	if st.Records != total {
		t.Fatalf("wrote %d records, want %d", st.Records, total)
	}
	// The whole point of group commit: far fewer fsyncs than records.
	if st.Fsyncs >= total {
		t.Fatalf("group commit did not amortise: %d fsyncs for %d records", st.Fsyncs, total)
	}
	if st.AvgBatchSize <= 1.0 {
		t.Fatalf("average batch size %.2f; no batching happened", st.AvgBatchSize)
	}
	t.Logf("%d records in %d batches (avg %.1f), %d fsyncs",
		st.Records, st.Batches, st.AvgBatchSize, st.Fsyncs)
}

func TestLSNsAreDenseAndOrderedUnderConcurrency(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}

	const writers, perWriter = 16, 200
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				if _, err := w.SubmitWait(Record{Type: RecSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
					t.Error(err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	w.Close()

	// On disk, LSNs must be strictly increasing with no gaps. A gap means
	// an LSN was assigned but its record never reached the file; an
	// out-of-order pair means the queue send raced the counter.
	var prev uint64
	count := 0
	_, err = Replay(dir, 0, false, func(r Record) error {
		if r.LSN != prev+1 {
			return fmt.Errorf("LSN %d follows %d", r.LSN, prev)
		}
		prev = r.LSN
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != writers*perWriter {
		t.Fatalf("replayed %d records, want %d", count, writers*perWriter)
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(dir)
	o.SegmentSize = 8 << 10 // tiny, to force many rotations
	w, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if _, err := w.SubmitWait(Record{
			Type: RecSet, Key: []byte(fmt.Sprintf("k%04d", i)), Value: bytes.Repeat([]byte("v"), 64),
		}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 3 {
		t.Fatalf("only %d segments after 500 records with an 8KiB limit", len(segs))
	}
	// Segments must be sorted and non-overlapping by first LSN.
	for i := 1; i < len(segs); i++ {
		if segs[i].FirstLSN <= segs[i-1].FirstLSN {
			t.Fatalf("segment %d first LSN %d does not follow %d", i, segs[i].FirstLSN, segs[i-1].FirstLSN)
		}
	}
	// And replay across all of them must still be dense.
	var prev uint64
	if _, err := Replay(dir, 0, false, func(r Record) error {
		if r.LSN != prev+1 {
			return fmt.Errorf("gap: %d follows %d", r.LSN, prev)
		}
		prev = r.LSN
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if prev != 500 {
		t.Fatalf("replay ended at LSN %d, want 500", prev)
	}
	t.Logf("%d segments created", len(segs))
}

func TestSegmentNameRoundTrip(t *testing.T) {
	for _, lsn := range []uint64{0, 1, 65537, 1 << 40} {
		name := SegmentName(lsn)
		got, ok := ParseSegmentName(name)
		if !ok || got != lsn {
			t.Fatalf("SegmentName(%d) = %q parsed back as %d, ok=%v", lsn, name, got, ok)
		}
	}
	for _, bad := range []string{"foo.log", "123.log", "0000000000000001.tmp", "snapshot"} {
		if _, ok := ParseSegmentName(bad); ok {
			t.Fatalf("%q parsed as a segment name", bad)
		}
	}
}

// --- recovery fault handling ----------------------------------------------

func writeSomeRecords(t *testing.T, dir string, n int, segSize int64) {
	t.Helper()
	o := testOptions(dir)
	o.SegmentSize = segSize
	w, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := w.SubmitWait(Record{
			Type: RecSet, Key: []byte(fmt.Sprintf("k%04d", i)), Value: []byte(fmt.Sprintf("v%04d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func lastSegment(t *testing.T, dir string) SegmentInfo {
	t.Helper()
	segs, err := ListSegments(dir)
	if err != nil || len(segs) == 0 {
		t.Fatalf("no segments in %s (err=%v)", dir, err)
	}
	return segs[len(segs)-1]
}

func TestTornTailIsTruncatedAndStartupContinues(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 100, 1<<20)

	// Chop the final record in half: exactly what a crash between write()
	// and completion looks like.
	seg := lastSegment(t, dir)
	if err := os.Truncate(seg.Path, seg.Size-12); err != nil {
		t.Fatal(err)
	}

	applied := 0
	res, err := Replay(dir, 0, false, func(Record) error { applied++; return nil })
	if err != nil {
		t.Fatalf("a torn tail must not prevent startup: %v", err)
	}
	if !res.Truncated {
		t.Fatal("torn tail was not reported as a truncation")
	}
	if applied != 99 {
		t.Fatalf("applied %d records, want 99 (the last one was torn)", applied)
	}
	// The file must actually be shorter now, and a second replay must be
	// clean — recovery has to be idempotent.
	res2, err := Replay(dir, 0, false, func(Record) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if res2.Truncated {
		t.Fatal("second replay still saw a torn tail; the truncation did not stick")
	}
}

func TestTrailingGarbageIsTreatedAsTornTail(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 50, 1<<20)
	seg := lastSegment(t, dir)

	f, err := os.OpenFile(seg.Path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Fewer than 8 bytes: cannot even be a record header.
	f.Write([]byte{0x01, 0x02, 0x03})
	f.Close()

	applied := 0
	res, err := Replay(dir, 0, false, func(Record) error { applied++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if applied != 50 {
		t.Fatalf("applied %d records, want 50", applied)
	}
	if !res.Truncated {
		t.Fatal("trailing garbage should have been truncated")
	}
}

func TestBitFlipInFinalSegmentRefusesByDefault(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 100, 1<<20)
	seg := lastSegment(t, dir)

	// Flip a bit in the middle of the file. This is a *complete* record
	// whose bytes changed, which is corruption, not a torn tail.
	data, err := os.ReadFile(seg.Path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0x08
	if err := os.WriteFile(seg.Path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Replay(dir, 0, false, func(Record) error { return nil }); !errors.Is(err, ErrCorruptMidLog) {
		t.Fatalf("got %v, want ErrCorruptMidLog — silently truncating real corruption is data loss", err)
	}

	// With the explicit opt-in, it must proceed and truncate.
	res, err := Replay(dir, 0, true, func(Record) error { return nil })
	if err != nil {
		t.Fatalf("--unsafe-truncate should allow startup: %v", err)
	}
	if !res.Truncated {
		t.Fatal("unsafe truncate did not truncate")
	}
}

func TestCorruptionInNonFinalSegmentRefuses(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 400, 4<<10) // force several segments

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 3 {
		t.Skipf("only %d segments produced; need at least 3", len(segs))
	}
	// Corrupt the FIRST segment.
	data, err := os.ReadFile(segs[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(segs[0].Path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = Replay(dir, 0, false, func(Record) error { return nil })
	if !errors.Is(err, ErrCorruptMidLog) {
		t.Fatalf("got %v, want ErrCorruptMidLog", err)
	}
}

func TestUnsafeTruncateRemovesLaterSegments(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 400, 4<<10)
	segs, _ := ListSegments(dir)
	if len(segs) < 3 {
		t.Skip("not enough segments")
	}
	data, _ := os.ReadFile(segs[0].Path)
	data[len(data)/2] ^= 0xFF
	os.WriteFile(segs[0].Path, data, 0o644)

	if _, err := Replay(dir, 0, true, func(Record) error { return nil }); err != nil {
		t.Fatal(err)
	}
	after, _ := ListSegments(dir)
	if len(after) != 1 {
		t.Fatalf("%d segments remain after unsafe truncate of the first; want 1", len(after))
	}
}

func TestReplaySkipsBelowSnapshotLSN(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 100, 1<<20)

	var applied []uint64
	res, err := Replay(dir, 60, false, func(r Record) error {
		applied = append(applied, r.LSN)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 40 {
		t.Fatalf("applied %d records past LSN 60, want 40", len(applied))
	}
	if applied[0] != 61 {
		t.Fatalf("first applied LSN is %d, want 61", applied[0])
	}
	if res.Skipped != 60 {
		t.Fatalf("Skipped = %d, want 60", res.Skipped)
	}
}

func TestReplayEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	res, err := Replay(dir, 0, false, func(Record) error {
		t.Fatal("no records should be replayed from an empty dir")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.NextLSN != 1 {
		t.Fatalf("NextLSN = %d for a fresh log, want 1", res.NextLSN)
	}
}

func TestEmptySegmentFileIsHandled(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 10, 1<<20)
	// Simulate a crash right after create(): the file exists, the header
	// never landed.
	empty := filepath.Join(dir, SegmentName(9999))
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	applied := 0
	if _, err := Replay(dir, 0, false, func(Record) error { applied++; return nil }); err != nil {
		t.Fatalf("a zero-length final segment must not block startup: %v", err)
	}
	if applied != 10 {
		t.Fatalf("applied %d records, want 10", applied)
	}
}

func TestResumeAppendsAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 50, 1<<20)

	res, err := Replay(dir, 0, false, func(Record) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	o := testOptions(dir)
	o.StartLSN = res.NextLSN
	o.ResumePath = res.ResumePath
	o.ResumeOffset = res.ResumeOffset
	w, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		if _, err := w.SubmitWait(Record{Type: RecSet, Key: []byte("post"), Value: []byte("recovery")}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	total := 0
	var prev uint64
	if _, err := Replay(dir, 0, false, func(r Record) error {
		if r.LSN != prev+1 {
			return fmt.Errorf("gap after resume: %d follows %d", r.LSN, prev)
		}
		prev = r.LSN
		total++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if total != 75 {
		t.Fatalf("total records after resume = %d, want 75", total)
	}
}

func TestTruncateBelowRemovesObsoleteSegments(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 400, 4<<10)
	segs, _ := ListSegments(dir)
	if len(segs) < 3 {
		t.Skip("not enough segments")
	}

	// Everything up to the third segment's first LSN is now in a snapshot.
	cutoff := segs[2].FirstLSN - 1
	removed, err := TruncateBelow(dir, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("removed %d segments, want 2", removed)
	}
	after, _ := ListSegments(dir)
	if len(after) != len(segs)-2 {
		t.Fatalf("%d segments remain, want %d", len(after), len(segs)-2)
	}
	// The surviving log must replay cleanly from the cutoff.
	if _, err := Replay(dir, cutoff, false, func(Record) error { return nil }); err != nil {
		t.Fatalf("replay after truncation failed: %v", err)
	}
}

func TestTruncateBelowNeverRemovesTheActiveSegment(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 20, 1<<20) // one segment only
	removed, err := TruncateBelow(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed %d segments; the only/active segment must survive", removed)
	}
}

func TestVerifyReportsFaults(t *testing.T) {
	dir := t.TempDir()
	writeSomeRecords(t, dir, 50, 1<<20)

	results, err := Verify(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || !results[0].Healthy() {
		t.Fatalf("healthy log reported as faulty: %+v", results)
	}
	if results[0].Records != 50 {
		t.Fatalf("Verify counted %d records, want 50", results[0].Records)
	}

	seg := lastSegment(t, dir)
	os.Truncate(seg.Path, seg.Size-5)
	results, err = Verify(dir)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Fault != FaultTornTail {
		t.Fatalf("fault = %v, want torn-tail", results[0].Fault)
	}
}

func TestWriterRejectsOversizedRecords(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if _, err := w.Submit(Record{Type: RecSet, Key: bytes.Repeat([]byte("k"), maxKeyLen+1)}); err == nil {
		t.Fatal("oversized key accepted")
	}
	if _, err := w.Submit(Record{Type: RecSet, Key: []byte("k"), Value: make([]byte, maxValLen+1)}); err == nil {
		t.Fatal("oversized value accepted")
	}
	if _, err := w.Submit(Record{Type: RecSegmentHdr, Key: segmentMagic}); err == nil {
		t.Fatal("a caller must not be able to forge a SEGMENT_HDR record")
	}
}

func TestSubmitAfterCloseFails(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Submit(Record{Type: RecSet, Key: []byte("k")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close must be idempotent: %v", err)
	}
}

func TestFsyncPolicies(t *testing.T) {
	for _, policy := range []config.FsyncPolicy{config.FsyncAlways, config.FsyncEverySec, config.FsyncNo} {
		t.Run(string(policy), func(t *testing.T) {
			dir := t.TempDir()
			o := testOptions(dir)
			o.Fsync = policy
			w, err := Open(o)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 200; i++ {
				if _, err := w.SubmitWait(Record{Type: RecSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
					t.Fatal(err)
				}
			}
			st := w.Stats()
			if st.Records != 200 {
				t.Fatalf("records = %d", st.Records)
			}
			if policy == config.FsyncAlways && st.Fsyncs == 0 {
				t.Fatal("always policy performed no fsyncs")
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			// Regardless of policy, a *clean* close must leave everything
			// readable: Close syncs.
			n := 0
			if _, err := Replay(dir, 0, false, func(Record) error { n++; return nil }); err != nil {
				t.Fatal(err)
			}
			if n != 200 {
				t.Fatalf("after clean close, replayed %d of 200 records", n)
			}
		})
	}
}

func BenchmarkWALAppend(b *testing.B) {
	for _, policy := range []config.FsyncPolicy{config.FsyncAlways, config.FsyncEverySec, config.FsyncNo} {
		b.Run(string(policy), func(b *testing.B) {
			dir := b.TempDir()
			o := testOptions(dir)
			o.Fsync = policy
			o.SegmentSize = 1 << 30
			w, err := Open(o)
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()
			val := bytes.Repeat([]byte("v"), 64)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := w.SubmitWait(Record{Type: RecSet, Key: []byte("bench-key"), Value: val}); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.StopTimer()
			st := w.Stats()
			b.ReportMetric(st.AvgBatchSize, "records/batch")
		})
	}
}
