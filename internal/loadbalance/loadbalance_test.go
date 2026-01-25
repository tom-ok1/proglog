package loadbalance_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	api "github.com/tom-ok1/proglog/api/v1"
	"github.com/tom-ok1/proglog/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestLoadBalancer(t *testing.T) {
	// Track which servers receive requests
	serverHits := &syncMap{m: make(map[string]int)}

	// Create leader and follower servers with trackable logs
	leaderLog := &trackingCommitLog{name: "leader", hits: serverHits}
	leader := setupTestServerWithLog(t, leaderLog)
	t.Cleanup(leader.srv.Stop)

	follower1Log := &trackingCommitLog{name: "follower-1", hits: serverHits}
	follower1 := setupTestServerWithLog(t, follower1Log)
	t.Cleanup(follower1.srv.Stop)

	follower2Log := &trackingCommitLog{name: "follower-2", hits: serverHits}
	follower2 := setupTestServerWithLog(t, follower2Log)
	t.Cleanup(follower2.srv.Stop)

	// Pre-populate logs with data for consume tests
	record := &api.Record{Value: []byte("test-data")}
	leaderLog.appendWithoutTracking(record)
	follower1Log.appendWithoutTracking(record)
	follower2Log.appendWithoutTracking(record)

	// Create discovery server that returns the cluster topology
	servers := []*api.Server{
		{Id: "leader", RpcAddr: leader.addr, IsLeader: true},
		{Id: "follower-1", RpcAddr: follower1.addr, IsLeader: false},
		{Id: "follower-2", RpcAddr: follower2.addr, IsLeader: false},
	}
	discovery := setupDiscoveryServer(t, servers)
	t.Cleanup(discovery.srv.Stop)

	// Create client using custom "proglog" resolver scheme
	conn, err := grpc.NewClient(
		"proglog://"+discovery.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	client := api.NewLogClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test 1: Produce requests should go to leader
	t.Run("produce routes to leader", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			_, err := client.Produce(ctx, &api.ProduceRequest{
				Record: &api.Record{Value: []byte("produce-test")},
			})
			if err != nil {
				t.Fatalf("Produce %d failed: %v", i, err)
			}
		}

		leaderHits := serverHits.get("leader-produce")
		if leaderHits != 5 {
			t.Errorf("leader received %d produce requests, expected 5", leaderHits)
		}
		if f1 := serverHits.get("follower-1-produce"); f1 > 0 {
			t.Errorf("follower-1 received %d produce requests, expected 0", f1)
		}
		if f2 := serverHits.get("follower-2-produce"); f2 > 0 {
			t.Errorf("follower-2 received %d produce requests, expected 0", f2)
		}
	})

	// Test 2: Consume requests should round-robin to followers
	t.Run("consume round-robins to followers", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			_, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
			if err != nil {
				t.Fatalf("Consume %d failed: %v", i, err)
			}
		}

		f1Hits := serverHits.get("follower-1-consume")
		f2Hits := serverHits.get("follower-2-consume")
		leaderHits := serverHits.get("leader-consume")

		if f1Hits == 0 {
			t.Error("follower-1 received no consume requests")
		}
		if f2Hits == 0 {
			t.Error("follower-2 received no consume requests")
		}
		if leaderHits > 0 {
			t.Errorf("leader received %d consume requests, expected 0", leaderHits)
		}
		t.Logf("consume distribution: leader=%d, follower-1=%d, follower-2=%d",
			leaderHits, f1Hits, f2Hits)
	})
}

func TestLoadBalancerNoFollowers(t *testing.T) {
	// Track which servers receive requests
	serverHits := &syncMap{m: make(map[string]int)}

	// Create only a leader (no followers)
	leaderLog := &trackingCommitLog{name: "leader", hits: serverHits}
	leader := setupTestServerWithLog(t, leaderLog)
	t.Cleanup(leader.srv.Stop)

	// Pre-populate log for consume tests
	record := &api.Record{Value: []byte("test-data")}
	leaderLog.appendWithoutTracking(record)

	// Discovery server returns only the leader
	servers := []*api.Server{
		{Id: "leader", RpcAddr: leader.addr, IsLeader: true},
	}
	discovery := setupDiscoveryServer(t, servers)
	t.Cleanup(discovery.srv.Stop)

	conn, err := grpc.NewClient(
		"proglog://"+discovery.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	client := api.NewLogClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// When no followers, consume should fall back to leader
	for i := 0; i < 5; i++ {
		_, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
		if err != nil {
			t.Fatalf("Consume %d failed: %v", i, err)
		}
	}

	leaderHits := serverHits.get("leader-consume")
	if leaderHits != 5 {
		t.Errorf("leader received %d consume requests, expected 5", leaderHits)
	}
}

// syncMap is a thread-safe map for tracking request hits
type syncMap struct {
	mu sync.Mutex
	m  map[string]int
}

func (s *syncMap) inc(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key]++
}

func (s *syncMap) get(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key]
}

// trackingCommitLog implements server.CommitLog and tracks which server receives requests
type trackingCommitLog struct {
	mu      sync.Mutex
	name    string
	hits    *syncMap
	records []*api.Record
}

func (m *trackingCommitLog) Append(record *api.Record) (uint64, error) {
	m.hits.inc(m.name + "-produce")
	m.mu.Lock()
	defer m.mu.Unlock()
	offset := uint64(len(m.records))
	record.Offset = offset
	m.records = append(m.records, record)
	return offset, nil
}

func (m *trackingCommitLog) appendWithoutTracking(record *api.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	offset := uint64(len(m.records))
	record.Offset = offset
	m.records = append(m.records, record)
}

func (m *trackingCommitLog) Read(offset uint64) (*api.Record, error) {
	m.hits.inc(m.name + "-consume")
	m.mu.Lock()
	defer m.mu.Unlock()
	if offset >= uint64(len(m.records)) {
		return nil, api.ErrOffsetOutOfRange{}
	}
	return m.records[offset], nil
}

// mockGetServerer implements server.GetServerer for testing
type mockGetServerer struct {
	servers []*api.Server
}

func (m *mockGetServerer) GetServers() ([]*api.Server, error) {
	return m.servers, nil
}

// mockCommitLog implements server.CommitLog for discovery server
type mockCommitLog struct {
	mu      sync.Mutex
	records []*api.Record
}

func (m *mockCommitLog) Append(record *api.Record) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	offset := uint64(len(m.records))
	record.Offset = offset
	m.records = append(m.records, record)
	return offset, nil
}

func (m *mockCommitLog) Read(offset uint64) (*api.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if offset >= uint64(len(m.records)) {
		return nil, api.ErrOffsetOutOfRange{}
	}
	return m.records[offset], nil
}

type testServer struct {
	srv  *grpc.Server
	addr string
}

func setupTestServerWithLog(t *testing.T, log *trackingCommitLog) testServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srvConfig := &server.Config{
		CommitLog:   log,
		GetServerer: &mockGetServerer{},
	}

	gsrv, err := server.NewGRPCServer(srvConfig)
	if err != nil {
		t.Fatal(err)
	}

	go gsrv.Serve(l)
	return testServer{srv: gsrv, addr: l.Addr().String()}
}

func setupDiscoveryServer(t *testing.T, servers []*api.Server) testServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srvConfig := &server.Config{
		CommitLog:   &mockCommitLog{},
		GetServerer: &mockGetServerer{servers: servers},
	}

	gsrv, err := server.NewGRPCServer(srvConfig)
	if err != nil {
		t.Fatal(err)
	}

	go gsrv.Serve(l)
	return testServer{srv: gsrv, addr: l.Addr().String()}
}
