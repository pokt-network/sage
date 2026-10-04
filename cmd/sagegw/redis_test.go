package main

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
)

// Under Sentinel the client is go-redis's failover client, asking the
// sentinels for the master; otherwise the one address. Either way the
// credentials, database and pool reach it, for the main client and the peer
// one alike.
func TestNewRedisClient(t *testing.T) {
	base := config.RedisConfig{Username: "sage", Password: "pw", DialTimeout: 2 * time.Second}

	plain := base
	plain.Address = "redis:6379"
	c := newRedisClient(&config.Config{Redis: plain}, 7, 10)
	defer c.Close()
	if o := c.Options(); o.Addr != "redis:6379" || o.DB != 7 || o.PoolSize != 10 || o.Username != "sage" || o.Password != "pw" {
		t.Errorf("plain client options %+v", o)
	}

	sentinel := base
	sentinel.SentinelMaster, sentinel.SentinelAddresses = "mymaster", []string{"s1:26379", "s2:26379"}
	f := newRedisClient(&config.Config{Redis: sentinel}, 6, 2)
	defer f.Close()
	if o := f.Options(); o.Addr != "FailoverClient" || o.DB != 6 || o.PoolSize != 2 || o.Username != "sage" || o.Password != "pw" {
		t.Errorf("sentinel client options %+v; want go-redis's failover client", o)
	}
	if got := redisTarget(&config.Config{Redis: sentinel}); got != "sentinel master mymaster via s1:26379,s2:26379" {
		t.Errorf("redisTarget = %q", got)
	}
}
