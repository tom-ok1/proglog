package log

import (
	"context"
	"sync"

	api "github.com/tom-ok1/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]context.CancelFunc
	closed      bool
	cancel      context.CancelFunc
	ctx         context.Context
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		r.logger.Info("replicator: already replicating to server", zap.String("name", name))
		return nil
	}

	ctx, cancel := context.WithCancel(r.ctx)
	r.servers[name] = cancel
	go r.replicate(ctx, name, addr)

	r.logger.Info("replicator: started replicating to server", zap.String("name", name))
	return nil
}

func (r *Replicator) replicate(ctx context.Context, name, addr string) {
	conn, err := grpc.NewClient(addr, r.DialOptions...)
	if err != nil {
		r.logger.Error("replicator: failed to dial server", zap.String("name", name), zap.Error(err))
		return
	}
	defer conn.Close()

	client := api.NewLogClient(conn)
	stream, err := client.ConsumeStream(ctx,
		&api.ConsumeRequest{Offset: 0})
	if err != nil {
		r.logger.Error("replicator: failed to create consume stream", zap.String("name", name), zap.Error(err))
		return
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Error("replicator: failed to receive record", zap.String("name", name), zap.Error(err))
			return
		}
		_, err = r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: resp.Record})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Error("replicator: failed to produce record", zap.String("name", name), zap.Error(err))
			return
		}
	}
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	cancel, ok := r.servers[name]
	if !ok {
		r.logger.Info("replicator: not replicating to server", zap.String("name", name))
		return nil
	}

	cancel()
	delete(r.servers, name)
	r.logger.Info("replicator: stopped replicating to server", zap.String("name", name))
	return nil
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	if r.cancel != nil {
		r.cancel()
	}
	r.closed = true
	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]context.CancelFunc)
	}
	if r.ctx == nil {
		r.ctx, r.cancel = context.WithCancel(context.Background())
	}
}
