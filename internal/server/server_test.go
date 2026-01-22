package server

import (
	"context"
	"io"
	"net"
	"testing"

	api "github.com/tom-ok1/proglog/api/v1"
	"github.com/tom-ok1/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"consume stream from offset succeeds":                testConsumeStreamFromOffset,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	clientOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cc, err := grpc.NewClient(l.Addr().String(), clientOptions...)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()

	clog, err := log.NewLog(dir, log.Config{})
	if err != nil {
		t.Fatal(err)
	}

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := newgrpcServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	gsrv := grpc.NewServer()
	api.RegisterLogServer(gsrv, server)

	go func() {
		gsrv.Serve(l)
	}()

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		gsrv.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	if err != nil {
		t.Fatalf("produce failed: %v", err)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	if string(consume.Record.Value) != string(want.Value) {
		t.Fatalf("consume.Record.Value = %s, want %s", consume.Record.Value, want.Value)
	}
	if consume.Record.Offset != produce.Offset {
		t.Fatalf("consume.Record.Offset = %d, want %d", consume.Record.Offset, produce.Offset)
	}
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	if err != nil {
		t.Fatalf("produce failed: %v", err)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatalf("consume should be nil, got %v", consume)
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}

	// Test ProduceStream
	{
		stream, err := client.ProduceStream(ctx)
		if err != nil {
			t.Fatalf("ProduceStream failed: %v", err)
		}

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			if err != nil {
				t.Fatalf("ProduceStream send failed: %v", err)
			}
			res, err := stream.Recv()
			if err != nil {
				t.Fatalf("ProduceStream recv failed: %v", err)
			}
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset %d, want %d", res.Offset, offset)
			}
		}
	}

	// Test ConsumeStream
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		if err != nil {
			t.Fatalf("ConsumeStream failed: %v", err)
		}

		for i, record := range records {
			res, err := stream.Recv()
			if err != nil {
				t.Fatalf("ConsumeStream recv failed: %v", err)
			}
			if string(res.Record.Value) != string(record.Value) {
				t.Fatalf("got record value %s, want %s", res.Record.Value, record.Value)
			}
			if res.Record.Offset != uint64(i) {
				t.Fatalf("got offset %d, want %d", res.Record.Offset, i)
			}
		}
	}
}

func testConsumeStreamFromOffset(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	// Produce multiple records
	records := []*api.Record{
		{Value: []byte("first message")},
		{Value: []byte("second message")},
		{Value: []byte("third message")},
	}

	for _, record := range records {
		_, err := client.Produce(ctx, &api.ProduceRequest{Record: record})
		if err != nil {
			t.Fatalf("produce failed: %v", err)
		}
	}

	// Start consuming from offset 1 (skip first message)
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 1})
	if err != nil {
		t.Fatalf("ConsumeStream failed: %v", err)
	}

	for i := 1; i < len(records); i++ {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ConsumeStream recv failed: %v", err)
		}
		if string(res.Record.Value) != string(records[i].Value) {
			t.Fatalf("got record value %s, want %s", res.Record.Value, records[i].Value)
		}
	}
}
