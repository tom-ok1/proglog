package log

import (
	"context"
	"sync"
	"testing"
	"time"

	api "github.com/tom-ok1/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestReplicator(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T){
		"join adds server":                   testReplicatorJoin,
		"join duplicate does not re-add":     testReplicatorJoinDuplicate,
		"leave removes server":               testReplicatorLeave,
		"leave non-existent is no-op":        testReplicatorLeaveNonExistent,
		"close cancels all":                  testReplicatorClose,
		"close is idempotent":                testReplicatorCloseIdempotent,
		"join after close returns early":     testReplicatorJoinAfterClose,
		"leave after close returns early":    testReplicatorLeaveAfterClose,
		"context cancelled stops replicate":  testReplicatorContextCancellation,
	} {
		t.Run(scenario, fn)
	}
}

func setupReplicator(t *testing.T) *Replicator {
	t.Helper()
	logger := zap.NewNop()
	return &Replicator{
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		LocalServer: &mockLogClient{},
		logger:      logger,
	}
}

func testReplicatorJoin(t *testing.T) {
	r := setupReplicator(t)
	defer r.Close()

	err := r.Join("server1", "localhost:1234")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(r.servers))
	}
	if _, ok := r.servers["server1"]; !ok {
		t.Fatal("expected server1 to be in servers map")
	}
}

func testReplicatorJoinDuplicate(t *testing.T) {
	r := setupReplicator(t)
	defer r.Close()

	err := r.Join("server1", "localhost:1234")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = r.Join("server1", "localhost:1234")
	if err != nil {
		t.Fatalf("expected no error on duplicate join, got %v", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.servers) != 1 {
		t.Fatalf("expected 1 server after duplicate join, got %d", len(r.servers))
	}
}

func testReplicatorLeave(t *testing.T) {
	r := setupReplicator(t)
	defer r.Close()

	err := r.Join("server1", "localhost:1234")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	r.Leave("server1")

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.servers) != 0 {
		t.Fatalf("expected 0 servers after leave, got %d", len(r.servers))
	}
}

func testReplicatorLeaveNonExistent(t *testing.T) {
	r := setupReplicator(t)
	defer r.Close()

	r.init()
	r.Leave("non-existent")

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.servers) != 0 {
		t.Fatalf("expected 0 servers, got %d", len(r.servers))
	}
}

func testReplicatorClose(t *testing.T) {
	r := setupReplicator(t)

	err := r.Join("server1", "localhost:1234")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	err = r.Join("server2", "localhost:1235")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	r.Close()

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		t.Fatal("expected replicator to be closed")
	}
}

func testReplicatorCloseIdempotent(t *testing.T) {
	r := setupReplicator(t)

	r.init()
	r.Close()
	r.Close()

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		t.Fatal("expected replicator to be closed")
	}
}

func testReplicatorJoinAfterClose(t *testing.T) {
	r := setupReplicator(t)
	r.init()
	r.Close()

	err := r.Join("server1", "localhost:1234")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.servers) != 0 {
		t.Fatalf("expected 0 servers after join on closed replicator, got %d", len(r.servers))
	}
}

func testReplicatorLeaveAfterClose(t *testing.T) {
	r := setupReplicator(t)

	err := r.Join("server1", "localhost:1234")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	r.Close()
	r.Leave("server1")
}

func testReplicatorContextCancellation(t *testing.T) {
	mockClient := &mockLogClient{}
	logger := zap.NewNop()
	r := &Replicator{
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		LocalServer: mockClient,
		logger:      logger,
	}
	r.init()

	ctx, cancel := context.WithCancel(r.ctx)

	done := make(chan struct{})
	go func() {
		r.replicate(ctx, "test", "localhost:9999")
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replicate did not exit after context cancellation")
	}

	r.Close()
}

type mockLogClient struct {
	mu       sync.Mutex
	produced []*api.Record
}

func (m *mockLogClient) Produce(ctx context.Context, in *api.ProduceRequest, opts ...grpc.CallOption) (*api.ProduceResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.produced = append(m.produced, in.Record)
	return &api.ProduceResponse{Offset: uint64(len(m.produced) - 1)}, nil
}

func (m *mockLogClient) ProduceStream(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[api.ProduceRequest, api.ProduceResponse], error) {
	return nil, nil
}

func (m *mockLogClient) Consume(ctx context.Context, in *api.ConsumeRequest, opts ...grpc.CallOption) (*api.ConsumeResponse, error) {
	return nil, nil
}

func (m *mockLogClient) ConsumeStream(ctx context.Context, in *api.ConsumeRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[api.ConsumeResponse], error) {
	return nil, nil
}
