# Claude Code Notes

## Testing Patterns in This Codebase

### General Style
- Use table-driven tests with `map[string]func(t *testing.T, ...)` pattern (see `server_test.go`, `membership_test.go`)
- Setup functions return teardown functions: `setupX(t) (result, func())`
- Use `t.Helper()` in all helper functions
- Use `t.TempDir()` for temporary directories (auto-cleaned)
- Use `t.Cleanup()` for cleanup that should run even on test failure

### Getting Free Ports/Addresses
```go
func getFreeAddr(t *testing.T) string {
    l, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        t.Fatalf("failed to get free addr: %v", err)
    }
    addr := l.Addr().String()
    l.Close()
    return addr
}
```

### gRPC Testing
- `grpc.NewClient` requires explicit transport credentials
- For tests without TLS: `grpc.WithTransportCredentials(insecure.NewCredentials())`
- The agent code needed to be updated to support insecure mode (fallback when `PeerTLSConfig` is nil)

### TLS in Tests
- TLS can be avoided for simpler tests if the code supports insecure mode
- When TLS is needed, use the `internal/config` package with `config.SetupTLSConfig()`
- Certificate files are resolved via `CONFIG_DIR` env var or `~/.proglog/`

### Serf/Membership Testing
- Serf produces verbose logs during tests - this is normal
- Cluster formation takes time - may need `time.Sleep()` for stabilization
- Events like `EventMemberJoin`, `EventMemberLeave`, `EventMemberFailed` are expected

### Replicator
- `Replicator.Close()` must check if `cancel` is nil before calling (fixed bug)
- Replicator uses `ConsumeStream` to replicate from remote servers
- Replication tests can be flaky due to timing - use retries with backoff

### Common Gotchas
- Always check if optional fields (like TLS configs) are nil before using
- Context cancellation in goroutines - check `ctx.Err() != nil` before logging errors
- gRPC connections need explicit credentials in newer versions
