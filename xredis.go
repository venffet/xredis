package xredis

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/venffet/redis-balancer"
	"gopkg.in/redis.v5"
)

// Redis nil reply, .e.g. when key does not exist.
const Nil = redis.Nil

type ClusterClient struct {
	*redis.Client // Just as a surface

	balance *balancer.Balancer
	clopt   *ClusterOptions
}

// all redis instance should with same config except ip address
type ClusterOptions struct {
	// host:port address.
	Addrs []string

	///////////// Balancer Config /////////////
	// copy from "redis-balancer/Options.go:Options"
	// Check interval, min 100ms, defaults to 1s
	CheckInterval time.Duration

	// Rise and Fall indicate the number of checks required to
	// mark the instance as up or down, defaults to 1
	Rise, Fall int

	// Balance Mode, ModeLeastConn is diabled, defaults to ModeRoundRobin
	Mode balancer.BalanceMode

	////////////// Failover Config ///////////
	// Maximum number of retries in failover.
	// Default is min(3, Number of Clients).
	FailoverRetries int

	////////////// Client config /////////////
	// copy from "gopkg.in/redis.v5/options.go:Options"
	// The network type, either tcp or unix.
	// Default is tcp.
	Network string

	// Dialer creates new network connection and has priority over
	// Network and Addr options.
	Dialer func() (net.Conn, error)

	// Optional password. Must match the password specified in the
	// requirepass server configuration option.
	Password string
	// Database to be selected after connecting to the server.
	DB int

	// Maximum number of retries before giving up.
	// Default is to not retry failed commands.
	MaxRetries int

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is 3 seconds.
	ReadTimeout time.Duration
	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is 3 seconds.
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 10 connections.
	PoolSize int
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout time.Duration
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is to not close idle connections.
	IdleTimeout time.Duration
	// Frequency of idle checks.
	// Default is 1 minute.
	// When minus value is set, then idle check is disabled.
	IdleCheckFrequency time.Duration

	// Enables read only queries on slave nodes.
	ReadOnly bool

	// TLS Config to use. When set TLS will be negotiated.
	TLSConfig *tls.Config
}

// init default value
func (clopt *ClusterOptions) init() {
	if clopt.FailoverRetries == 0 {
		clopt.FailoverRetries = 3
	}
	if clopt.FailoverRetries > len(clopt.Addrs) {
		clopt.FailoverRetries = len(clopt.Addrs)
	}
	if clopt.Mode == 0 {
		clopt.Mode = balancer.ModeRoundRobin
	}
}

// return a fix Client
// in case that you need a fix Client to do some command
func (c *ClusterClient) Next() *redis.Client { return c.balance.Next() }

// NOTE: ModeLeastConn is diabled
// for compatible with twemproxy
// redis-balance in "https://github.com/venffet/tree/master/redis-balancer"
// has disable the ModeLeastConn mode, if you want the mode, use redis-balance in github
// and compile with xredis again.
//
func NewClusterClient(clopt *ClusterOptions) *ClusterClient {
	clopt.init()
	opts := make([]balancer.Options, 0, len(clopt.Addrs))

	for i := 0; i < len(clopt.Addrs); i++ {
		opt := balancer.Options{
			Options: redis.Options{
				Network:            clopt.Network,
				Addr:               clopt.Addrs[i],
				Dialer:             clopt.Dialer,
				Password:           clopt.Password,
				DB:                 clopt.DB,
				MaxRetries:         0,
				DialTimeout:        clopt.DialTimeout,
				ReadTimeout:        clopt.ReadTimeout,
				PoolSize:           clopt.PoolSize,
				PoolTimeout:        clopt.PoolTimeout,
				IdleTimeout:        clopt.IdleTimeout,
				IdleCheckFrequency: clopt.IdleCheckFrequency,
				//ReadOnly:           clopt.ReadOnly,							// not support read on slave nodes
				TLSConfig: clopt.TLSConfig,
			},
			CheckInterval: clopt.CheckInterval,
			Rise:          clopt.Rise,
			Fall:          clopt.Fall,
		}
		opts = append(opts, opt)
	}

	cluster := ClusterClient{
		Client:  redis.NewClient(&opts[0].Options), // just use client 0 to init the Client, we don't use this Client directly.
		balance: balancer.New(opts, clopt.Mode),
		clopt:   clopt,
	}

	// delegate all cmd to "ClusterClient:Process"
	cluster.Client.WrapProcess(func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
		return cluster.Process
	})

	return &cluster
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////
// Method to wrap ClusterClient as a "redis.Client"

// Override "redis.Client:Process"
func (c *ClusterClient) Process(cmd redis.Cmder) error {
	var err error

	// if fail, failover to next available Client,
	for i := 0; i <= c.clopt.FailoverRetries; i++ {
		client := c.Next()
		for j := 0; j <= c.clopt.MaxRetries; j++ {
			err = client.Process(cmd)
			if err == nil || err == redis.Nil {
				return err
			}
		}
	}
	return err
}

// Override "redis.Client:PoolStats"
// PoolStats returns accumulated connection pool stats.
func (c *ClusterClient) PoolStats() *redis.PoolStats {
	var acc redis.PoolStats
	for _, backend := range c.balance.GetSelector() {
		s := backend.GetClient().PoolStats()
		acc.Requests += s.Requests
		acc.Hits += s.Hits
		acc.Timeouts += s.Timeouts
		acc.TotalConns += s.TotalConns
		acc.FreeConns += s.FreeConns
	}
	return &acc
}

// Override "redis.Client:Pipelined"
func (c *ClusterClient) Pipelined(fn func(*redis.Pipeline) error) ([]redis.Cmder, error) {
	return c.Next().Pipelined(fn)
}

// Override "redis.Client:Pipelined"
func (c *ClusterClient) Pipeline() *redis.Pipeline {
	return c.Next().Pipeline()
}

// Override "redis.Client:Close"
func (c *ClusterClient) Close() error { return c.balance.Close() }

//////////////////////////////////////////////////////////////////////////////////////////////////////////
