package discovery

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
)

type mockHandler struct {
	joins  chan serf.Member
	leaves chan serf.Member
}

func (h *mockHandler) HandleJoin(member serf.Member) {
	if h.joins != nil {
		h.joins <- member
	}
}

func (h *mockHandler) HandleLeave(member serf.Member) {
	if h.leaves != nil {
		h.leaves <- member
	}
}

func TestMembership(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, members []*Membership){
		"join and leave events are handled": testJoinLeave,
		"members returns all cluster nodes":  testMembers,
	} {
		t.Run(scenario, func(t *testing.T) {
			members, handler := setupMembership(t, 3)
			defer func() {
				for _, m := range members {
					m.Leave()
				}
			}()
			fn(t, members)
			_ = handler
		})
	}
}

func setupMembership(t *testing.T, count int) ([]*Membership, *mockHandler) {
	t.Helper()

	members := make([]*Membership, count)
	handler := &mockHandler{
		joins:  make(chan serf.Member, 16),
		leaves: make(chan serf.Member, 16),
	}

	addrs := make([]string, count)
	for i := range count {
		addrs[i] = getFreeAddr(t)
	}

	for i := range count {
		config := Config{
			NodeName: fmt.Sprintf("node-%d", i),
			BindAddr: addrs[i],
			Tags: map[string]string{
				"rpc_addr": addrs[i],
			},
		}

		if i > 0 {
			config.StartJoinAddrs = []string{addrs[0]}
		}

		m, err := New(handler, config)
		if err != nil {
			t.Fatalf("failed to create membership %d: %v", i, err)
		}

		members[i] = m
	}

	return members, handler
}

func getFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func testJoinLeave(t *testing.T, members []*Membership) {
	handler := members[0].handler.(*mockHandler)

	// Wait for join events from other nodes (node-1 and node-2 joining)
	// Each node sees joins from other nodes
	timeout := time.After(5 * time.Second)
	expectedJoins := 2 // node-1 and node-2 joining as seen by node-0

	joins := 0
	for joins < expectedJoins {
		select {
		case <-handler.joins:
			joins++
		case <-timeout:
			t.Fatalf("timed out waiting for join events, got %d expected %d", joins, expectedJoins)
		}
	}

	// Now have node-2 leave
	if err := members[2].Leave(); err != nil {
		t.Fatalf("failed to leave: %v", err)
	}

	// Wait for leave event
	timeout = time.After(5 * time.Second)
	select {
	case member := <-handler.leaves:
		if member.Name != "node-2" {
			t.Fatalf("expected node-2 to leave, got %s", member.Name)
		}
	case <-timeout:
		t.Fatal("timed out waiting for leave event")
	}
}

func testMembers(t *testing.T, members []*Membership) {
	// Give time for cluster to stabilize
	time.Sleep(1 * time.Second)

	for i, m := range members {
		memberList := m.Members()
		if len(memberList) != len(members) {
			t.Fatalf("node-%d: expected %d members, got %d", i, len(members), len(memberList))
		}
	}

	// Verify all nodes are present
	nodeNames := make(map[string]bool)
	for _, member := range members[0].Members() {
		nodeNames[member.Name] = true
	}

	for i := range len(members) {
		name := fmt.Sprintf("node-%d", i)
		if !nodeNames[name] {
			t.Fatalf("expected node %s to be in members list", name)
		}
	}
}
