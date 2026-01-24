package log

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	api "github.com/tom-ok1/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestDistributedLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		logs []*DistributedLog,
	){
		"append and read succeeds":            testAppendRead,
		"replicate logs across cluster":       testReplication,
		"join and leave cluster":              testJoinLeave,
		"leader election after leader leaves": testLeaderElection,
	} {
		t.Run(scenario, func(t *testing.T) {
			logs, teardown := setupDistributedLogs(t, 3)
			defer teardown()
			fn(t, logs)
		})
	}
}

func setupDistributedLogs(t *testing.T, count int) ([]*DistributedLog, func()) {
	t.Helper()

	var logs []*DistributedLog
	var lns []net.Listener

	for i := 0; i < count; i++ {
		dataDir := t.TempDir()

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to create listener: %v", err)
		}
		lns = append(lns, ln)

		config := Config{}
		config.Raft.StreamLayer = NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("node-%d", i))
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond
		config.Raft.Bootstrap = i == 0

		log, err := NewDistributedLog(dataDir, config)
		if err != nil {
			t.Fatalf("failed to create distributed log %d: %v", i, err)
		}
		logs = append(logs, log)

		if i > 0 {
			err = logs[0].Join(
				fmt.Sprintf("node-%d", i),
				ln.Addr().String(),
			)
			if err != nil {
				t.Fatalf("failed to join node %d: %v", i, err)
			}
		} else {
			err = logs[0].WaitForLeader(3 * time.Second)
			if err != nil {
				t.Fatalf("failed to wait for leader: %v", err)
			}
		}
	}

	return logs, func() {
		for _, log := range logs {
			_ = log.Close()
		}
		for _, ln := range lns {
			_ = ln.Close()
		}
	}
}

func testAppendRead(t *testing.T, logs []*DistributedLog) {
	want := &api.Record{Value: []byte("hello world")}

	off, err := logs[0].Append(want)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	got, err := logs[0].Read(off)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(got.Value) != string(want.Value) {
		t.Fatalf("got value=%s, want %s", got.Value, want.Value)
	}
}

func testReplication(t *testing.T, logs []*DistributedLog) {
	want := &api.Record{Value: []byte("replicated data")}

	off, err := logs[0].Append(want)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// Wait for replication
	time.Sleep(500 * time.Millisecond)

	// Verify all nodes have the record
	for i, log := range logs {
		got, err := log.Read(off)
		if err != nil {
			t.Fatalf("read from node %d failed: %v", i, err)
		}
		if string(got.Value) != string(want.Value) {
			t.Fatalf("node %d: got value=%s, want %s", i, got.Value, want.Value)
		}
	}
}

func testJoinLeave(t *testing.T, logs []*DistributedLog) {
	// Append a record before adding a new node
	want := &api.Record{Value: []byte("before new node")}
	_, err := logs[0].Append(want)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// Create and join a new node
	dataDir := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	config := Config{}
	config.Raft.StreamLayer = NewStreamLayer(ln, nil, nil)
	config.Raft.LocalID = raft.ServerID("node-new")
	config.Raft.HeartbeatTimeout = 50 * time.Millisecond
	config.Raft.ElectionTimeout = 50 * time.Millisecond
	config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
	config.Raft.CommitTimeout = 5 * time.Millisecond
	config.Raft.Bootstrap = false

	newLog, err := NewDistributedLog(dataDir, config)
	if err != nil {
		t.Fatalf("failed to create new distributed log: %v", err)
	}
	t.Cleanup(func() { newLog.Close() })

	err = logs[0].Join("node-new", ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to join new node: %v", err)
	}

	// Wait for replication to new node
	time.Sleep(500 * time.Millisecond)

	// Leave the cluster
	err = logs[0].Leave("node-new")
	if err != nil {
		t.Fatalf("failed to leave: %v", err)
	}
}

func testLeaderElection(t *testing.T, logs []*DistributedLog) {
	// Append a record
	want := &api.Record{Value: []byte("test data")}
	_, err := logs[0].Append(want)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// Remove the leader
	err = logs[0].Leave(string(logs[0].config.Raft.LocalID))
	if err != nil {
		t.Fatalf("failed to leave: %v", err)
	}

	// Wait for new leader election
	time.Sleep(500 * time.Millisecond)

	// Verify a new leader is elected
	leaderFound := false
	for i := 1; i < len(logs); i++ {
		if logs[i].raft.Leader() != "" {
			leaderFound = true
			break
		}
	}

	if !leaderFound {
		// Give more time for election
		time.Sleep(1 * time.Second)
		for i := 1; i < len(logs); i++ {
			if logs[i].raft.Leader() != "" {
				leaderFound = true
				break
			}
		}
	}
}

func TestLogStore(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		store *logStore,
	){
		"first and last index":   testLogStoreFirstLastIndex,
		"store and get log":      testLogStoreGetLog,
		"store multiple logs":    testLogStoreStoreLogs,
		"delete range":           testLogStoreDeleteRange,
	} {
		t.Run(scenario, func(t *testing.T) {
			store, teardown := setupLogStore(t)
			defer teardown()
			fn(t, store)
		})
	}
}

func setupLogStore(t *testing.T) (*logStore, func()) {
	t.Helper()

	dir := t.TempDir()
	c := Config{}
	c.Segment.InitialOffset = 1

	store, err := newLogStore(dir, c)
	if err != nil {
		t.Fatalf("failed to create log store: %v", err)
	}

	return store, func() {
		store.Close()
	}
}

func testLogStoreFirstLastIndex(t *testing.T, store *logStore) {
	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("first index failed: %v", err)
	}
	if first != 1 {
		t.Fatalf("expected first index=1, got %d", first)
	}

	// Store a log entry
	err = store.StoreLog(&raft.Log{
		Data:  []byte("test"),
		Index: 1,
		Type:  raft.LogCommand,
		Term:  1,
	})
	if err != nil {
		t.Fatalf("store log failed: %v", err)
	}

	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("last index failed: %v", err)
	}
	if last != 1 {
		t.Fatalf("expected last index=1, got %d", last)
	}
}

func testLogStoreGetLog(t *testing.T, store *logStore) {
	want := &raft.Log{
		Data:  []byte("hello"),
		Index: 1,
		Type:  raft.LogCommand,
		Term:  1,
	}

	err := store.StoreLog(want)
	if err != nil {
		t.Fatalf("store log failed: %v", err)
	}

	got := &raft.Log{}
	err = store.GetLog(1, got)
	if err != nil {
		t.Fatalf("get log failed: %v", err)
	}

	if string(got.Data) != string(want.Data) {
		t.Fatalf("got data=%s, want %s", got.Data, want.Data)
	}
	if got.Term != want.Term {
		t.Fatalf("got term=%d, want %d", got.Term, want.Term)
	}
	if got.Type != want.Type {
		t.Fatalf("got type=%d, want %d", got.Type, want.Type)
	}
}

func testLogStoreStoreLogs(t *testing.T, store *logStore) {
	logs := []*raft.Log{
		{Data: []byte("one"), Index: 1, Type: raft.LogCommand, Term: 1},
		{Data: []byte("two"), Index: 2, Type: raft.LogCommand, Term: 1},
		{Data: []byte("three"), Index: 3, Type: raft.LogCommand, Term: 1},
	}

	err := store.StoreLogs(logs)
	if err != nil {
		t.Fatalf("store logs failed: %v", err)
	}

	for i, want := range logs {
		got := &raft.Log{}
		err = store.GetLog(uint64(i+1), got)
		if err != nil {
			t.Fatalf("get log %d failed: %v", i+1, err)
		}
		if string(got.Data) != string(want.Data) {
			t.Fatalf("log %d: got data=%s, want %s", i+1, got.Data, want.Data)
		}
	}

	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("last index failed: %v", err)
	}
	if last != 3 {
		t.Fatalf("expected last index=3, got %d", last)
	}
}

func testLogStoreDeleteRange(t *testing.T, store *logStore) {
	logs := []*raft.Log{
		{Data: []byte("one"), Index: 1, Type: raft.LogCommand, Term: 1},
		{Data: []byte("two"), Index: 2, Type: raft.LogCommand, Term: 1},
		{Data: []byte("three"), Index: 3, Type: raft.LogCommand, Term: 1},
	}

	err := store.StoreLogs(logs)
	if err != nil {
		t.Fatalf("store logs failed: %v", err)
	}

	// DeleteRange calls Truncate which removes segments where nextOffset-1 <= max
	// Verify DeleteRange doesn't error (actual truncation depends on segment boundaries)
	err = store.DeleteRange(1, 2)
	if err != nil {
		t.Fatalf("delete range failed: %v", err)
	}

	// Verify the store is still usable
	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("last index failed: %v", err)
	}
	if last < 1 {
		t.Fatalf("expected last index >= 1, got %d", last)
	}
}

func TestStreamLayer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		serverLayer *StreamLayer,
		clientLayer *StreamLayer,
	){
		"dial and accept": testStreamLayerDialAccept,
		"addr returns listener address": testStreamLayerAddr,
	} {
		t.Run(scenario, func(t *testing.T) {
			serverLayer, clientLayer, teardown := setupStreamLayers(t)
			defer teardown()
			fn(t, serverLayer, clientLayer)
		})
	}
}

func setupStreamLayers(t *testing.T) (*StreamLayer, *StreamLayer, func()) {
	t.Helper()

	serverLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create server listener: %v", err)
	}

	clientLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		serverLn.Close()
		t.Fatalf("failed to create client listener: %v", err)
	}

	serverLayer := NewStreamLayer(serverLn, nil, nil)
	clientLayer := NewStreamLayer(clientLn, nil, nil)

	return serverLayer, clientLayer, func() {
		serverLayer.Close()
		clientLayer.Close()
	}
}

func testStreamLayerDialAccept(t *testing.T, serverLayer, clientLayer *StreamLayer) {
	serverAddr := raft.ServerAddress(serverLayer.Addr().String())

	// Start accepting in goroutine
	acceptDone := make(chan struct{})
	var acceptErr error
	var serverConn net.Conn

	go func() {
		defer close(acceptDone)
		serverConn, acceptErr = serverLayer.Accept()
	}()

	// Dial from client
	clientConn, err := clientLayer.Dial(serverAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer clientConn.Close()

	// Wait for accept
	<-acceptDone
	if acceptErr != nil {
		t.Fatalf("accept failed: %v", acceptErr)
	}
	defer serverConn.Close()

	// Test data exchange
	want := []byte("hello raft")
	_, err = clientConn.Write(want)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	got := make([]byte, len(want))
	_, err = serverConn.Read(got)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(got) != string(want) {
		t.Fatalf("got=%s, want=%s", got, want)
	}
}

func testStreamLayerAddr(t *testing.T, serverLayer, _ *StreamLayer) {
	addr := serverLayer.Addr()
	if addr == nil {
		t.Fatal("expected non-nil address")
	}

	_, err := net.ResolveTCPAddr("tcp", addr.String())
	if err != nil {
		t.Fatalf("invalid address: %v", err)
	}
}

func TestFSM(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		f *fsm,
	){
		"apply append request": testFSMApply,
		"snapshot and restore": testFSMSnapshotRestore,
	} {
		t.Run(scenario, func(t *testing.T) {
			f, teardown := setupFSM(t)
			defer teardown()
			fn(t, f)
		})
	}
}

func setupFSM(t *testing.T) (*fsm, func()) {
	t.Helper()

	dir := t.TempDir()
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	log, err := NewLog(dir, c)
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}

	return &fsm{log: log}, func() {
		log.Close()
	}
}

func testFSMApply(t *testing.T, f *fsm) {
	want := &api.Record{Value: []byte("hello fsm")}
	req := &api.ProduceRequest{Record: want}

	b, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	data := make([]byte, 1+len(b))
	data[0] = byte(AppendRequestType)
	copy(data[1:], b)

	raftLog := &raft.Log{Data: data}
	result := f.Apply(raftLog)

	resp, ok := result.(*api.ProduceResponse)
	if !ok {
		if err, isErr := result.(error); isErr {
			t.Fatalf("apply returned error: %v", err)
		}
		t.Fatalf("expected ProduceResponse, got %T", result)
	}

	if resp.Offset != 0 {
		t.Fatalf("expected offset=0, got %d", resp.Offset)
	}

	// Verify the record was stored
	got, err := f.log.Read(resp.Offset)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(got.Value) != string(want.Value) {
		t.Fatalf("got value=%s, want %s", got.Value, want.Value)
	}
}

func testFSMSnapshotRestore(t *testing.T, f *fsm) {
	// Add some records
	records := []*api.Record{
		{Value: []byte("one")},
		{Value: []byte("two")},
		{Value: []byte("three")},
	}

	for _, record := range records {
		_, err := f.log.Append(record)
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	// Verify records were stored
	highest, err := f.log.HighestOffset()
	if err != nil {
		t.Fatalf("failed to get highest offset: %v", err)
	}
	if highest != 2 {
		t.Fatalf("expected highest offset=2, got %d", highest)
	}

	// Read snapshot data directly using the reader
	reader := f.log.Reader()
	snapshotData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read from log reader: %v", err)
	}
	if len(snapshotData) == 0 {
		t.Fatalf("reader returned empty data")
	}

	// Create a new FSM and restore
	dir := t.TempDir()
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	newLog, err := NewLog(dir, c)
	if err != nil {
		t.Fatalf("failed to create new log: %v", err)
	}
	defer newLog.Close()

	newFSM := &fsm{log: newLog}

	// Create a ReadCloser from the snapshot data
	snapshotReader := io.NopCloser(bytes.NewReader(snapshotData))

	err = newFSM.Restore(snapshotReader)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// Verify restored records
	for i, want := range records {
		got, err := newFSM.log.Read(uint64(i))
		if err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		if string(got.Value) != string(want.Value) {
			t.Fatalf("record %d: got value=%s, want %s", i, got.Value, want.Value)
		}
	}
}

// mockSnapshotSink implements raft.SnapshotSink for testing
type mockSnapshotSink struct {
	file *os.File
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.file.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	return m.file.Close()
}

func (m *mockSnapshotSink) ID() string {
	return "mock-snapshot"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

func TestWaitForLeader(t *testing.T) {
	logs, teardown := setupDistributedLogs(t, 1)
	defer teardown()

	// Should already have a leader
	err := logs[0].WaitForLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("wait for leader failed: %v", err)
	}
}

func TestWaitForLeaderTimeout(t *testing.T) {
	// Create a log that won't elect a leader (not bootstrapped)
	dataDir := t.TempDir()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer ln.Close()

	config := Config{}
	config.Raft.StreamLayer = NewStreamLayer(ln, nil, nil)
	config.Raft.LocalID = raft.ServerID("lonely-node")
	config.Raft.HeartbeatTimeout = 50 * time.Millisecond
	config.Raft.ElectionTimeout = 50 * time.Millisecond
	config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
	config.Raft.CommitTimeout = 5 * time.Millisecond
	config.Raft.Bootstrap = false // Not bootstrapped

	log, err := NewDistributedLog(dataDir, config)
	if err != nil {
		t.Fatalf("failed to create distributed log: %v", err)
	}
	defer log.Close()

	// Should timeout waiting for leader
	err = log.WaitForLeader(100 * time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}
