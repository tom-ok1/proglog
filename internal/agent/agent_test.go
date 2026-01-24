package agent

import (
	"context"
	"fmt"
	"net"
	"testing"

	api "github.com/tom-ok1/proglog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestAgent(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, agents []*Agent){
		"produce and consume through agents": testProduceConsume,
	} {
		t.Run(scenario, func(t *testing.T) {
			agents, teardown := setupAgents(t, 3)
			defer teardown()
			fn(t, agents)
		})
	}
}

func TestAgentShutdown(t *testing.T) {
	agents, teardown := setupAgents(t, 1)
	defer teardown()

	c := client(t, agents[0])
	ctx := context.Background()

	_, err := c.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("test")}})
	if err != nil {
		t.Fatalf("produce failed: %v", err)
	}

	// Shutdown the agent
	err = agents[0].Shutdown()
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Verify double shutdown is safe
	err = agents[0].Shutdown()
	if err != nil {
		t.Fatalf("second shutdown should not error: %v", err)
	}
}

func setupAgents(t *testing.T, count int) ([]*Agent, func()) {
	t.Helper()

	var agents []*Agent

	for i := range count {
		bindAddr := getFreeAddr(t)
		rpcPort := getFreePort(t)

		dataDir := t.TempDir()

		var startJoinAddrs []string
		if i > 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := New(Config{
			NodeName:       fmt.Sprintf("node-%d", i),
			BindAddr:       bindAddr,
			AdvertiseAddr:  bindAddr,
			RPCPort:        rpcPort,
			DataDir:        dataDir,
			StartJoinAddrs: startJoinAddrs,
		})
		if err != nil {
			t.Fatalf("failed to create agent %d: %v", i, err)
		}

		agents = append(agents, agent)
	}

	return agents, func() {
		for _, agent := range agents {
			_ = agent.Shutdown()
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

func client(t *testing.T, agent *Agent) api.LogClient {
	t.Helper()

	rpcAddr, err := agent.Config.ListenRPCAddr()
	if err != nil {
		t.Fatalf("failed to get rpc addr: %v", err)
	}

	conn, err := grpc.NewClient(rpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return api.NewLogClient(conn)
}

func testProduceConsume(t *testing.T, agents []*Agent) {
	leaderClient := client(t, agents[0])

	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}

	produce, err := leaderClient.Produce(ctx, &api.ProduceRequest{Record: want})
	if err != nil {
		t.Fatalf("produce failed: %v", err)
	}

	consume, err := leaderClient.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	if string(consume.Record.Value) != string(want.Value) {
		t.Fatalf("consume.Record.Value = %s, want %s", consume.Record.Value, want.Value)
	}
}

func TestConfigListenRPCAddr(t *testing.T) {
	tests := []struct {
		name     string
		bindAddr string
		rpcPort  int
		want     string
		wantErr  bool
	}{
		{
			name:     "valid address",
			bindAddr: "127.0.0.1:8080",
			rpcPort:  9090,
			want:     "127.0.0.1:9090",
			wantErr:  false,
		},
		{
			name:     "localhost",
			bindAddr: "localhost:8080",
			rpcPort:  9090,
			want:     "localhost:9090",
			wantErr:  false,
		},
		{
			name:     "invalid address",
			bindAddr: "invalid",
			rpcPort:  9090,
			want:     "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				BindAddr: tt.bindAddr,
				RPCPort:  tt.rpcPort,
			}
			got, err := c.ListenRPCAddr()
			if (err != nil) != tt.wantErr {
				t.Errorf("ListenRPCAddr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ListenRPCAddr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigAdvertiseRPCAddr(t *testing.T) {
	tests := []struct {
		name          string
		advertiseAddr string
		rpcPort       int
		want          string
		wantErr       bool
	}{
		{
			name:          "valid address",
			advertiseAddr: "192.168.1.1:8080",
			rpcPort:       9090,
			want:          "192.168.1.1:9090",
			wantErr:       false,
		},
		{
			name:          "invalid address",
			advertiseAddr: "not-valid",
			rpcPort:       9090,
			want:          "",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				AdvertiseAddr: tt.advertiseAddr,
				RPCPort:       tt.rpcPort,
			}
			got, err := c.AdvertiseRPCAddr()
			if (err != nil) != tt.wantErr {
				t.Errorf("AdvertiseRPCAddr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AdvertiseRPCAddr() = %v, want %v", got, tt.want)
			}
		})
	}
}
