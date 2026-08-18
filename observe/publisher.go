package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/pokt-network/sage/internal/safego"
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

// Publish enqueues an observation, dropping it with an error when the buffer
// is full rather than blocking. The observation pipeline is best-effort and
// sampled; a slow consumer must never back-pressure onto the relay path.
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

// Subscribe returns the shared buffered channel. Every subscriber reads from
// the same channel, so observations are distributed among consumers rather
// than broadcast to all of them.
func (p *ChannelPublisher) Subscribe(_ context.Context) (<-chan Observation, error) {
	return p.ch, nil
}

// Close stops further publishing. It is idempotent, and deliberately does not
// close the observation channel: subscribers may still be draining what is
// already buffered, and closing under them would be a send on a closed channel
// for any publisher racing the shutdown.
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

// Publish broadcasts an observation to every instance subscribed to the shared
// channel. Note that RequestBody and ResponseBody do not survive the trip —
// they are json:"-" on Observation, since shipping full relay bodies over
// pub/sub would dwarf the observation itself.
func (p *RedisPublisher) Publish(ctx context.Context, obs Observation) error {
	data, err := json.Marshal(obs)
	if err != nil {
		return fmt.Errorf("marshal observation: %w", err)
	}
	return p.client.Publish(ctx, redisChannel, data).Err()
}

// Subscribe returns a channel fed by Redis pub/sub, unmarshalling as it goes.
// Undecodable messages and sends to a full channel are both dropped silently:
// a subscriber that cannot keep up should lose observations, not stall the
// pub/sub reader for everyone else.
//
// The client must support Subscribe, which redis.Cmdable does not expose —
// hence the type assertion, and the error when it fails.
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
		defer safego.Recover(nil, "observe.subscribe")
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

// Close tears down the pub/sub subscription, which ends the reader goroutine
// and closes the channel handed to the subscriber. It is a no-op if Subscribe
// was never called.
func (p *RedisPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pubsub != nil {
		return p.pubsub.Close()
	}
	return nil
}
