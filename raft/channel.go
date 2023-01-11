package raft

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/niukuo/ragit/logging"
	"google.golang.org/grpc"
)

type RpcChannel interface {
	Invoke(ctx context.Context, method string, req, resp interface{}) error
	Update(lead PeerID)
	ResetConn()
	Describe(w io.Writer)
	Close()
}

type Channel struct {
	dialOptions   []grpc.DialOption
	reqTimeout    time.Duration
	retryInterval time.Duration

	storage Storage

	tolead atomic.Value

	lock  sync.RWMutex
	conns []*grpc.ClientConn
	lead  PeerID

	resetCh chan struct{}
	stopCh  chan struct{}

	logger logging.Logger
}

func NewChannel(storage Storage) *Channel {

	c := &Channel{
		dialOptions:   make([]grpc.DialOption, 0),
		reqTimeout:    5 * time.Second,
		retryInterval: 3 * time.Second,

		storage: storage,

		conns: make([]*grpc.ClientConn, 0),

		resetCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),

		logger: logging.GetLogger("raft.channel"),
	}

	go c.run()

	return c
}

func (c *Channel) Invoke(ctx context.Context, method string, req, resp interface{}) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, c.reqTimeout)
	defer cancel()

	for _, conn := range c.conns {
		if err := conn.Invoke(ctx, method, req, resp); err != nil {
			c.logger.Warning("Invoke failed",
				", target: ", conn.Target(),
				", method: ", method,
				", err: ", err,
			)
			continue
		}

		return nil
	}

	return fmt.Errorf("no available channel, cnt: %d", len(c.conns))
}

func (c *Channel) Update(lead PeerID) {
	if lead == PeerID(0) {
		c.logger.Warning("no leader")
		return
	}

	c.logger.Info("lead updated to ", lead.String())
	c.tolead.Store(lead)
}

func (c *Channel) run() {
	ticker := time.NewTicker(c.retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.refresh(); err != nil {
				c.logger.Warning("refresh err: ", err)
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *Channel) refresh() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	select {
	case <-c.resetCh:
		c.logger.Warning("reset conn, cnt: ", len(c.conns))
		for _, conn := range c.conns {
			conn.Close()
		}
		c.lead = PeerID(0)
		c.conns = make([]*grpc.ClientConn, 0)
	default:
	}

	tolead, ok := c.tolead.Load().(PeerID)
	if !ok || tolead == PeerID(0) || tolead == c.lead && len(c.conns) > 0 {
		return nil
	}

	c.lead = tolead
	c.conns = make([]*grpc.ClientConn, 0)

	lastIndex, err := c.storage.LastIndex()
	if err != nil {
		return err
	}
	if lastIndex == 0 {
		return nil
	}

	us, err := c.storage.GetURLsByMemberID(c.lead)
	if err != nil {
		return err
	}

	for _, u := range us {

		r, err := url.Parse(u)
		if err != nil {
			c.logger.Warningf("url.Parse %s, err: %v", u, err)
			continue
		}

		addr := r.Host

		conn, err := func(addr string) (*grpc.ClientConn, error) {
			dctx, cancel := context.WithTimeout(context.Background(), c.reqTimeout)
			defer cancel()

			return grpc.DialContext(dctx, addr, c.dialOptions...)

		}(addr)

		if err != nil {
			c.logger.Warningf("dial %s, err: %v", addr, err)
			continue
		}

		c.conns = append(c.conns, conn)
	}

	return nil
}

func (c *Channel) ResetConn() {
	select {
	case c.resetCh <- struct{}{}:
		c.tolead.Store(PeerID(0))
	default:
	}
}

func (c *Channel) Close() {
	select {
	case <-c.stopCh:
	default:
		c.logger.Warning("closing channel")
		close(c.stopCh)

		c.lock.Lock()
		defer c.lock.Unlock()

		for _, conn := range c.conns {
			conn.Close()
		}
	}
}

func (c *Channel) Describe(w io.Writer) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	fmt.Fprintf(w, "lead: %s\n", c.lead.String())
	fmt.Fprintf(w, "conns: %d\n", len(c.conns))

	for _, conn := range c.conns {
		fmt.Fprintf(w, "target: %s, state: %s\n", conn.Target(), conn.GetState())
	}
}
