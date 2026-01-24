package log

import (
	"io"
	"os"
	"testing"

	api "github.com/tom-ok1/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		log *Log,
	){
		"append and read succeeds":              testLogAppendRead,
		"read out of range returns error":       testLogOutOfRangeErr,
		"reopen preserves existing entries":     testLogInitExisting,
		"multiple segments created when needed": testLogWithMultipleSegments,
		"initial offset configuration works":    testLogWithInitialOffset,
		"reader returns all data":               testLogReader,
		"truncate removes old segments":         testLogTruncate,
		"remove deletes log directory":          testLogRemove,
		"default config applies defaults":       testNewLogWithDefaultConfig,
		"lowest and highest offset tracking":    testLogLowestAndHighestOffset,
	} {
		t.Run(scenario, func(t *testing.T) {
			log, teardown := setupLog(t)
			defer teardown()
			fn(t, log)
		})
	}
}

func setupLog(t *testing.T) (
	log *Log,
	teardown func(),
) {
	t.Helper()

	dir, err := os.MkdirTemp("", "log_test")
	if err != nil {
		t.Fatal(err)
	}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	log, err = NewLog(dir, c)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	return log, func() {
		log.Close()
		os.RemoveAll(dir)
	}
}

func testLogAppendRead(t *testing.T, log *Log) {
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

func testLogOutOfRangeErr(t *testing.T, log *Log) {
	_, err := log.Append(&api.Record{Value: []byte("hello world")})
	if err != nil {
		t.Fatal(err)
	}

	highestOff, err := log.HighestOffset()
	if err != nil {
		t.Fatal(err)
	}

	_, err = log.Read(highestOff + 1)
	if err == nil {
		t.Fatal("expected error when reading out of range")
	}
}

func testLogInitExisting(t *testing.T, log *Log) {
	want := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := log.Append(want)
		if err != nil {
			t.Fatal(err)
		}
	}

	lowestBefore, err := log.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	highestBefore, err := log.HighestOffset()
	if err != nil {
		t.Fatal(err)
	}

	dir := log.Dir
	c := log.Config

	err = log.Close()
	if err != nil {
		t.Fatal(err)
	}

	newLog, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer newLog.Close()

	lowestAfter, err := newLog.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if lowestAfter != lowestBefore {
		t.Fatalf("expected lowestOffset=%d, got %d", lowestBefore, lowestAfter)
	}

	highestAfter, err := newLog.HighestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if highestAfter != highestBefore {
		t.Fatalf("expected highestOffset=%d, got %d", highestBefore, highestAfter)
	}

	for off := lowestAfter; off <= highestAfter; off++ {
		record, err := newLog.Read(off)
		if err != nil {
			t.Fatalf("failed to read record at offset %d: %v", off, err)
		}
		if string(record.Value) != string(want.Value) {
			t.Fatalf("expected value=%s, got %s", want.Value, record.Value)
		}
	}
}

func testLogWithMultipleSegments(t *testing.T, log *Log) {
	// Close the default log and create one with small segment sizes
	dir := log.Dir
	log.Close()
	os.RemoveAll(dir)

	dir, err := os.MkdirTemp("", "log_multi_segment_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 32
	c.Segment.MaxIndexBytes = uint64(entWidth) * 3

	log, err = NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	want := &api.Record{Value: []byte("hello")}

	var offsets []uint64
	for i := 0; i < 10; i++ {
		off, err := log.Append(want)
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, off)
	}

	for _, off := range offsets {
		got, err := log.Read(off)
		if err != nil {
			t.Fatalf("failed to read record at offset %d: %v", off, err)
		}
		if string(got.Value) != string(want.Value) {
			t.Fatalf("expected value=%s, got %s", want.Value, got.Value)
		}
	}

	if len(log.segments) <= 1 {
		t.Fatal("expected multiple segments")
	}
}

func testLogWithInitialOffset(t *testing.T, log *Log) {
	// Close the default log and create one with initial offset
	dir := log.Dir
	log.Close()
	os.RemoveAll(dir)

	dir, err := os.MkdirTemp("", "log_initial_offset_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024
	c.Segment.InitialOffset = 100

	log, err = NewLog(dir, c)
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

func testLogReader(t *testing.T, log *Log) {
	want := &api.Record{Value: []byte("hello world")}
	_, err := log.Append(want)
	if err != nil {
		t.Fatal(err)
	}

	reader := log.Reader()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) <= 8 {
		t.Fatal("expected data to contain record with length prefix")
	}

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

func testLogTruncate(t *testing.T, log *Log) {
	// Close the default log and create one with small segment sizes
	dir := log.Dir
	log.Close()
	os.RemoveAll(dir)

	dir, err := os.MkdirTemp("", "log_truncate_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 32
	c.Segment.MaxIndexBytes = uint64(entWidth) * 2

	log, err = NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	want := &api.Record{Value: []byte("hi")}

	for i := 0; i < 6; i++ {
		_, err := log.Append(want)
		if err != nil {
			t.Fatal(err)
		}
	}

	initialSegmentCount := len(log.segments)
	if initialSegmentCount <= 1 {
		t.Fatalf("expected multiple segments, got %d", initialSegmentCount)
	}

	err = log.Truncate(2)
	if err != nil {
		t.Fatal(err)
	}

	if len(log.segments) >= initialSegmentCount {
		t.Fatalf("expected fewer segments after truncate, had %d, now have %d",
			initialSegmentCount, len(log.segments))
	}
}

func testLogRemove(t *testing.T, log *Log) {
	_, err := log.Append(&api.Record{Value: []byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	dir := log.Dir

	err = log.Remove()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("expected log directory to be removed")
	}
}

func testNewLogWithDefaultConfig(t *testing.T, log *Log) {
	// Close the default log and create one with empty config
	dir := log.Dir
	log.Close()
	os.RemoveAll(dir)

	dir, err := os.MkdirTemp("", "log_default_config_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	log, err = NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	if log.Config.Segment.MaxStoreBytes != 1024 {
		t.Fatalf("expected default MaxStoreBytes=1024, got %d", log.Config.Segment.MaxStoreBytes)
	}
	if log.Config.Segment.MaxIndexBytes != 1024 {
		t.Fatalf("expected default MaxIndexBytes=1024, got %d", log.Config.Segment.MaxIndexBytes)
	}

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

func testLogLowestAndHighestOffset(t *testing.T, log *Log) {
	// Close the default log and create one with initial offset
	dir := log.Dir
	log.Close()
	os.RemoveAll(dir)

	dir, err := os.MkdirTemp("", "log_offset_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024
	c.Segment.InitialOffset = 10

	log, err = NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	lowestOff, err := log.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if lowestOff != 10 {
		t.Fatalf("expected lowestOffset=10, got %d", lowestOff)
	}

	want := &api.Record{Value: []byte("test")}
	for i := 0; i < 5; i++ {
		_, err := log.Append(want)
		if err != nil {
			t.Fatal(err)
		}
	}

	lowestOff, err = log.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if lowestOff != 10 {
		t.Fatalf("expected lowestOffset=10, got %d", lowestOff)
	}

	highestOff, err := log.HighestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if highestOff != 14 {
		t.Fatalf("expected highestOffset=14, got %d", highestOff)
	}
}
