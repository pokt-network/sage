package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

const redisChannel = "sage:observations"

// Publisher supports multi-instance observation sharing.
type Publisher interface {
	Publish(ctx context.Context, obs Observation) error
	Subscribe(ctx context.Context) (<-chan Observation, error)
	Close() error
}

// ChannelPublisher is an in-process Publisher backed by a Go channel.
// Useful for testing and single-instance deployments.
type ChannelPublisher struct {
	ch     chan Observation
	once   sync.Once
	closed chan struct{}
}

// NewChannelPublisher creates an in-process publisher with the given buffer size.
func NewChannelPublisher(bufSize int) *ChannelPublisher {
	if bufSize <= 0 {
		bufSize = 64
	}
	return &ChannelPublisher{
		ch:     make(chan Observation, bufSize),
		closed: make(chan struct{}),
	}
}

func (p *ChannelPublisher) Publish(_ context.Context, obs Observation) error {
	// Check closed first (non-blocking).
	select {
	case <-p.closed:
		return fmt.Errorf("publisher closed")
	default:
	}

	select {
	case p.ch <- obs:
		return nil
	default:
		return fmt.Errorf("publisher channel full")
	}
}

func (p *ChannelPublisher) Subscribe(_ context.Context) (<-chan Observation, error) {
	return p.ch, nil
}

func (p *ChannelPublisher) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

// RedisPublisher shares observations across instances via Redis pub/sub.
type RedisPublisher struct {
	client redis.Cmdable
	pubsub *redis.PubSub
	mu     sync.Mutex
}

// NewRedisPublisher creates a Redis-backed publisher.
func NewRedisPublisher(client redis.Cmdable) *RedisPublisher {
	return &RedisPublisher{client: client}
}

func (p *RedisPublisher) Publish(ctx context.Context, obs Observation) error {
	data, err := json.Marshal(obs)
	if err != nil {
		return fmt.Errorf("marshal observation: %w", err)
	}
	return p.client.Publish(ctx, redisChannel, data).Err()
}

func (p *RedisPublisher) Subscribe(ctx context.Context) (<-chan Observation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Use the underlying *redis.Client to subscribe.
	// redis.Cmdable doesn't expose Subscribe, so we type-assert.
	type subscriber interface {
		Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	}
	sub, ok := p.client.(subscriber)
	if !ok {
		return nil, fmt.Errorf("redis client does not support Subscribe")
	}

	p.pubsub = sub.Subscribe(ctx, redisChannel)

	ch := make(chan Observation, 64)
	go func() {
		defer close(ch)
		for msg := range p.pubsub.Channel() {
			var obs Observation
			if err := json.Unmarshal([]byte(msg.Payload), &obs); err != nil {
				continue
			}
			select {
			case ch <- obs:
			default:
				// Drop if consumer is slow.
			}
		}
	}()

	return ch, nil
}

func (p *RedisPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pubsub != nil {
		return p.pubsub.Close()
	}
	return nil
}
