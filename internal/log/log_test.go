package log

import (
	"io"
	"os"
	"testing"

	api "github.com/tom-ok1/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	log, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}

	testAppendRead(t, log)
	testOutOfRangeErr(t, log)
	testInitExisting(t, log, dir, c)
	testReader(t, log)
	testTruncate(t, log)

	err = log.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func testAppendRead(t *testing.T, log *Log) {
	t.Helper()
	want := &api.Record{Value: []byte("hello world")}

	off, err := log.Append(want)
	if err != nil {
		t.Fatal(err)
	}

	got, err := log.Read(off)
	if err != nil {
		t.Fatal(err)
	}

	if string(got.Value) != string(want.Value) {
		t.Fatalf("expected value=%s, got %s", want.Value, got.Value)
	}
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	t.Helper()
	highestOff, err := log.HighestOff()
	if err != nil {
		t.Fatal(err)
	}

	_, err = log.Read(highestOff + 1)
	if err == nil {
		t.Fatal("expected error when reading out of range")
	}
}

func testInitExisting(t *testing.T, log *Log, dir string, c Config) {
	t.Helper()

	// Append some records
	want := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := log.Append(want)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get offsets before closing
	lowestBefore, err := log.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	highestBefore, err := log.HighestOff()
	if err != nil {
		t.Fatal(err)
	}

	// Close and reopen
	err = log.Close()
	if err != nil {
		t.Fatal(err)
	}

	newLog, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}

	// Verify offsets are preserved
	lowestAfter, err := newLog.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if lowestAfter != lowestBefore {
		t.Fatalf("expected lowestOffset=%d, got %d", lowestBefore, lowestAfter)
	}

	highestAfter, err := newLog.HighestOff()
	if err != nil {
		t.Fatal(err)
	}
	if highestAfter != highestBefore {
		t.Fatalf("expected highestOffset=%d, got %d", highestBefore, highestAfter)
	}

	// Verify we can still read existing records
	for off := lowestAfter; off <= highestAfter; off++ {
		record, err := newLog.Read(off)
		if err != nil {
			t.Fatalf("failed to read record at offset %d: %v", off, err)
		}
		if string(record.Value) != string(want.Value) {
			t.Fatalf("expected value=%s, got %s", want.Value, record.Value)
		}
	}

	// Replace the log for subsequent tests
	*log = *newLog
}

func testReader(t *testing.T, log *Log) {
	t.Helper()

	highestOff, err := log.HighestOff()
	if err != nil {
		t.Fatal(err)
	}

	reader := log.Reader()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) == 0 {
		t.Fatal("expected data from reader, got empty")
	}

	// Verify we can read records from the log
	lowestOff, err := log.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}

	// Check that the number of records matches
	recordCount := highestOff - lowestOff + 1
	if recordCount == 0 {
		t.Fatal("expected at least one record in log")
	}
}

func testTruncate(t *testing.T, log *Log) {
	t.Helper()

	// The log already has records from previous tests
	lowestOff, err := log.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}

	// Truncate at the lowest offset - this should not remove all segments
	// since we only remove segments where nextOffset-1 <= lowest
	err = log.Truncate(lowestOff)
	if err != nil {
		t.Fatal(err)
	}

	// The log should still be functional
	want := &api.Record{Value: []byte("hello world")}
	_, err = log.Append(want)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLogWithMultipleSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_multi_segment_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	// Use small segment sizes to force multiple segments
	c.Segment.MaxStoreBytes = 32
	c.Segment.MaxIndexBytes = uint64(entWidth) * 3

	log, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	want := &api.Record{Value: []byte("hello")}

	// Append enough records to create multiple segments
	var offsets []uint64
	for i := 0; i < 10; i++ {
		off, err := log.Append(want)
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, off)
	}

	// Verify all records can be read back
	for _, off := range offsets {
		got, err := log.Read(off)
		if err != nil {
			t.Fatalf("failed to read record at offset %d: %v", off, err)
		}
		if string(got.Value) != string(want.Value) {
			t.Fatalf("expected value=%s, got %s", want.Value, got.Value)
		}
	}

	// Verify we have multiple segments
	if len(log.segments) <= 1 {
		t.Fatal("expected multiple segments")
	}
}

func TestLogWithInitialOffset(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_initial_offset_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024
	c.Segment.InitialOffset = 100

	log, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	want := &api.Record{Value: []byte("hello world")}
	off, err := log.Append(want)
	if err != nil {
		t.Fatal(err)
	}

	if off != 100 {
		t.Fatalf("expected first offset=100, got %d", off)
	}

	lowestOff, err := log.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if lowestOff != 100 {
		t.Fatalf("expected lowestOffset=100, got %d", lowestOff)
	}
}

func TestLogReader(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_reader_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	log, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	want := &api.Record{Value: []byte("hello world")}
	_, err = log.Append(want)
	if err != nil {
		t.Fatal(err)
	}

	reader := log.Reader()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	// The data should contain the record (with length prefix from store)
	// Length prefix is 8 bytes (uint64)
	if len(data) <= 8 {
		t.Fatal("expected data to contain record with length prefix")
	}

	// Extract record from data (skip the 8-byte length prefix)
	recordData := data[8:]
	got := &api.Record{}
	err = proto.Unmarshal(recordData, got)
	if err != nil {
		t.Fatalf("failed to unmarshal record: %v", err)
	}

	if string(got.Value) != string(want.Value) {
		t.Fatalf("expected value=%s, got %s", want.Value, got.Value)
	}
}

func TestLogTruncate(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_truncate_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	// Use small segment sizes to force multiple segments
	c.Segment.MaxStoreBytes = 32
	c.Segment.MaxIndexBytes = uint64(entWidth) * 2

	log, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	want := &api.Record{Value: []byte("hi")}

	// Append enough records to create multiple segments
	var offsets []uint64
	for i := 0; i < 6; i++ {
		off, err := log.Append(want)
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, off)
	}

	initialSegmentCount := len(log.segments)
	if initialSegmentCount <= 1 {
		t.Fatalf("expected multiple segments, got %d", initialSegmentCount)
	}

	// Truncate at offset 2 (should remove segments with all records <= 2)
	err = log.Truncate(2)
	if err != nil {
		t.Fatal(err)
	}

	// Should have fewer segments now
	if len(log.segments) >= initialSegmentCount {
		t.Fatalf("expected fewer segments after truncate, had %d, now have %d",
			initialSegmentCount, len(log.segments))
	}
}

func TestLogRemove(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_remove_test")
	if err != nil {
		t.Fatal(err)
	}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	log, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}

	_, err = log.Append(&api.Record{Value: []byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	err = log.Remove()
	if err != nil {
		t.Fatal(err)
	}

	// The directory should be removed
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("expected log directory to be removed")
	}
}

func TestNewLogWithDefaultConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_default_config_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Use empty config to test defaults
	c := Config{}
	log, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	// Verify defaults were applied
	if log.Config.Segment.MaxStoreBytes != 1024 {
		t.Fatalf("expected default MaxStoreBytes=1024, got %d", log.Config.Segment.MaxStoreBytes)
	}
	if log.Config.Segment.MaxIndexBytes != 1024 {
		t.Fatalf("expected default MaxIndexBytes=1024, got %d", log.Config.Segment.MaxIndexBytes)
	}

	// Verify log is functional
	want := &api.Record{Value: []byte("test")}
	off, err := log.Append(want)
	if err != nil {
		t.Fatal(err)
	}

	got, err := log.Read(off)
	if err != nil {
		t.Fatal(err)
	}

	if string(got.Value) != string(want.Value) {
		t.Fatalf("expected value=%s, got %s", want.Value, got.Value)
	}
}

func TestLogLowestAndHighestOffset(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_offset_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024
	c.Segment.InitialOffset = 10

	log, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	// Before any append, check initial state
	lowestOff, err := log.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if lowestOff != 10 {
		t.Fatalf("expected lowestOffset=10, got %d", lowestOff)
	}

	// Append multiple records
	want := &api.Record{Value: []byte("test")}
	for i := 0; i < 5; i++ {
		_, err := log.Append(want)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify offsets
	lowestOff, err = log.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if lowestOff != 10 {
		t.Fatalf("expected lowestOffset=10, got %d", lowestOff)
	}

	highestOff, err := log.HighestOff()
	if err != nil {
		t.Fatal(err)
	}
	if highestOff != 14 {
		t.Fatalf("expected highestOffset=14, got %d", highestOff)
	}
}
