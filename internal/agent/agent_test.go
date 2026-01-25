package agent_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	api "github.com/tom-ok1/proglog/api/v1"
	"github.com/tom-ok1/proglog/internal/agent"
	"github.com/tom-ok1/proglog/internal/loadbalance"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestAgent(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		agents []*agent.Agent,
		clients []api.LogClient,
	){
		"produce/consume succeeds":        testProduceConsume,
		"replicate across cluster":        testReplicate,
		"leader handles produce requests": testLeaderProduces,
	} {
		t.Run(scenario, func(t *testing.T) {
			agents, clients, teardown := setupAgents(t, 3)
			defer teardown()
			fn(t, agents, clients)
		})
	}
}

func setupAgents(t *testing.T, count int) (
	[]*agent.Agent,
	[]api.LogClient,
	func(),
) {
	t.Helper()

	agents := make([]*agent.Agent, count)
	clients := make([]api.LogClient, count)
	conns := make([]*grpc.ClientConn, count)

	// Get all bind addresses upfront
	bindAddrs := make([]string, count)
	rpcPorts := make([]int, count)

	for i := range count {
		bindAddrs[i] = getFreeAddr(t)
		rpcPorts[i] = getFreePort(t)
	}

	for i := range count {
		dataDir := t.TempDir()

		var startJoinAddrs []string
		if i > 0 {
			startJoinAddrs = []string{bindAddrs[0]}
		}

		config := agent.Config{
			NodeName:       fmt.Sprintf("node-%d", i),
			Bootstrap:      i == 0,
			StartJoinAddrs: startJoinAddrs,
			BindAddr:       bindAddrs[i],
			RPCPort:        rpcPorts[i],
			DataDir:        dataDir,
			// Use nil TLS configs for testing (insecure mode)
			ServerTLSConfig: nil,
			PeerTLSConfig:   nil,
		}

		a, err := agent.New(config)
		if err != nil {
			t.Fatalf("failed to create agent %d: %v", i, err)
		}
		agents[i] = a

		// Create gRPC client
		rpcAddr := fmt.Sprintf("127.0.0.1:%d", rpcPorts[i])
		conn, err := grpc.NewClient(
			fmt.Sprintf("%s:///%s", loadbalance.Name, rpcAddr),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("failed to create grpc client %d: %v", i, err)
		}
		conns[i] = conn
		clients[i] = api.NewLogClient(conn)
	}

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	return agents, clients, func() {
		for _, conn := range conns {
			conn.Close()
		}
		for _, a := range agents {
			a.Shutdown()
		}
	}
}

func getFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free addr: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func testProduceConsume(t *testing.T, agents []*agent.Agent, clients []api.LogClient) {
	ctx := context.Background()

	// Use the leader (first node) to produce
	want := &api.Record{Value: []byte("hello world")}

	produceRes, err := clients[0].Produce(ctx, &api.ProduceRequest{Record: want})
	if err != nil {
		t.Fatalf("produce failed: %v", err)
	}

	// wait for replication
	time.Sleep(3 * time.Second)

	consumeRes, err := clients[0].Consume(ctx, &api.ConsumeRequest{Offset: produceRes.Offset})
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	if string(consumeRes.Record.Value) != string(want.Value) {
		t.Fatalf("got value=%s, want %s", consumeRes.Record.Value, want.Value)
	}
}

func testReplicate(t *testing.T, agents []*agent.Agent, clients []api.LogClient) {
	ctx := context.Background()

	// Produce to the leader
	want := &api.Record{Value: []byte("replicated message")}

	produceRes, err := clients[0].Produce(ctx, &api.ProduceRequest{Record: want})
	if err != nil {
		t.Fatalf("produce failed: %v", err)
	}

	// Wait for replication
	time.Sleep(3 * time.Second)

	// Verify all nodes can read the record
	for i, client := range clients {
		consumeRes, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produceRes.Offset})
		if err != nil {
			t.Fatalf("consume from node %d failed: %v", i, err)
		}

		if string(consumeRes.Record.Value) != string(want.Value) {
			t.Fatalf("node %d: got value=%s, want %s", i, consumeRes.Record.Value, want.Value)
		}
	}
}

func testLeaderProduces(t *testing.T, agents []*agent.Agent, clients []api.LogClient) {
	ctx := context.Background()

	// Produce multiple records
	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
		{Value: []byte("third")},
	}

	for i, record := range records {
		_, err := clients[0].Produce(ctx, &api.ProduceRequest{Record: record})
		if err != nil {
			t.Fatalf("produce %d failed: %v", i, err)
		}
	}

	// Wait for replication
	time.Sleep(3 * time.Second)

	// Verify all records exist on all nodes
	for nodeIdx, client := range clients {
		for offset, want := range records {
			consumeRes, err := client.Consume(ctx, &api.ConsumeRequest{Offset: uint64(offset)})
			if err != nil {
				t.Fatalf("node %d consume offset %d failed: %v", nodeIdx, offset, err)
			}

			if string(consumeRes.Record.Value) != string(want.Value) {
				t.Fatalf("node %d offset %d: got value=%s, want %s",
					nodeIdx, offset, consumeRes.Record.Value, want.Value)
			}
		}
	}
}

func TestAgentShutdown(t *testing.T) {
	agents, _, teardown := setupAgents(t, 1)
	defer teardown()

	// Shutdown should succeed without error
	err := agents[0].Shutdown()
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Calling shutdown again should be idempotent
	err = agents[0].Shutdown()
	if err != nil {
		t.Fatalf("second shutdown failed: %v", err)
	}
}
