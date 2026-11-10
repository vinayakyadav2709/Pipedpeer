package rtctransport

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	maxWriteChunk = 16 << 10 // bytes per data-channel message
	readScratch   = 64 << 10 // largest message a peer may send
	// streamWindow is how many bytes one connection may have sent that the
	// peer's reader has not consumed yet. It bounds the memory a slow reader
	// costs, per connection, on the receiving side.
	streamWindow = 256 << 10
	// A reader returns credit once it has consumed this much.
	creditBatch = streamWindow / 2
)

var (
	errWindowExceeded = errors.New("rtctransport: peer sent past its window")
	errBadCredit      = errors.New("rtctransport: peer sent an invalid credit message")
)

// channel is a detached data channel. Binary messages carry stream bytes and
// text messages carry credit: the decimal count of bytes the sender may add to
// its window.
type channel interface {
	ReadDataChannel(p []byte) (n int, isText bool, err error)
	WriteDataChannel(p []byte, isText bool) (int, error)
	SetReadDeadline(time.Time) error
	Close() error
}

// conn is a net.Conn over one data channel.
//
// Every channel to a peer shares one SCTP association and so one receive
// window. If unread bytes were left in it, a single stalled reader would
// freeze every other connection to that peer. So pump always drains the
// channel into recv, and the per-connection credit window is what pushes back
// on the sender. The window is also the only bound on what a sender queues:
// a channel write never blocks, it only appends to the association's queue.
type conn struct {
	raw     channel
	local   Addr
	remote  Addr
	onClose func()

	readDeadline  deadline
	writeDeadline deadline
	readable      chan struct{}
	writable      chan struct{}
	closed        chan struct{}
	closeOnce     sync.Once
	closeErr      error
	writing       sync.Mutex

	mu       sync.Mutex
	recv     bytes.Buffer
	recvErr  error
	consumed int // bytes read from recv and not yet returned as credit
	credit   int // bytes this end may still send
}

func newConn(raw channel, local, remote Addr, onClose func()) *conn {
	c := &conn{
		raw:      raw,
		local:    local,
		remote:   remote,
		onClose:  onClose,
		readable: make(chan struct{}, 1),
		writable: make(chan struct{}, 1),
		closed:   make(chan struct{}),
		credit:   streamWindow,
	}
	go c.pump()
	return c
}

func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (c *conn) pump() {
	scratch := make([]byte, readScratch)
	for {
		n, isText, err := c.raw.ReadDataChannel(scratch)
		c.mu.Lock()
		switch {
		case err != nil:
			c.recvErr = err
		case isText:
			credit, perr := strconv.ParseUint(string(scratch[:n]), 10, 31)
			// An honest peer only returns credit for bytes it was sent.
			if perr != nil || c.credit+int(credit) > streamWindow {
				c.recvErr = errBadCredit
				break
			}
			c.credit += int(credit)
		case c.recv.Len()+n > streamWindow:
			c.recvErr = errWindowExceeded
		default:
			c.recv.Write(scratch[:n])
		}
		failed := c.recvErr != nil
		c.mu.Unlock()
		wake(c.readable)
		wake(c.writable)
		if failed {
			if err == nil {
				// Nothing drains the channel any more, so stop the peer
				// sending into the shared receive window.
				_ = c.raw.Close()
			}
			return
		}
	}
}

func (c *conn) Read(p []byte) (int, error) {
	for {
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		case <-c.readDeadline.expired():
			return 0, os.ErrDeadlineExceeded
		default:
		}
		c.mu.Lock()
		if c.recv.Len() > 0 {
			n, _ := c.recv.Read(p)
			c.consumed += n
			grant := 0
			if c.consumed >= creditBatch {
				grant, c.consumed = c.consumed, 0
			}
			more := c.recv.Len() > 0
			c.mu.Unlock()
			if more {
				wake(c.readable)
			}
			if grant > 0 {
				c.grant(grant)
			}
			return n, nil
		}
		err := c.recvErr
		c.mu.Unlock()
		if err != nil {
			return 0, err
		}
		select {
		case <-c.readable:
		case <-c.closed:
		case <-c.readDeadline.expired():
		}
	}
}

// grant tells the peer it may send n more bytes. A grant that cannot be sent
// is kept for the next Read: a lost one would stall the peer's writer for good.
func (c *conn) grant(n int) {
	if _, err := c.raw.WriteDataChannel(strconv.AppendInt(nil, int64(n), 10), true); err != nil {
		c.mu.Lock()
		c.consumed += n
		c.mu.Unlock()
	}
}

func (c *conn) Write(p []byte) (int, error) {
	c.writing.Lock()
	defer c.writing.Unlock()
	written := 0
	for len(p) > 0 {
		n, err := c.reserve(min(len(p), maxWriteChunk))
		if err != nil {
			return written, err
		}
		if _, err := c.raw.WriteDataChannel(p[:n], false); err != nil {
			select {
			case <-c.closed:
				return written, net.ErrClosed
			default:
				return written, err
			}
		}
		written += n
		p = p[n:]
	}
	return written, nil
}

// reserve takes up to want bytes of send credit, waiting for the peer to
// grant some when there is none.
func (c *conn) reserve(want int) (int, error) {
	for {
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		case <-c.writeDeadline.expired():
			return 0, os.ErrDeadlineExceeded
		default:
		}
		c.mu.Lock()
		n := min(want, c.credit)
		c.credit -= n
		err := c.recvErr
		c.mu.Unlock()
		if n > 0 {
			return n, nil
		}
		if err != nil {
			// No more credit can arrive once the receiving side has ended.
			if errors.Is(err, io.EOF) {
				err = io.ErrClosedPipe
			}
			return 0, err
		}
		select {
		case <-c.writable:
		case <-c.closed:
		case <-c.writeDeadline.expired():
		}
	}
}

func (c *conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		// Closing a channel only resets its outgoing side: the pump would
		// stay blocked until the peer answers. An expired deadline ends
		// it now.
		_ = c.raw.SetReadDeadline(time.Unix(1, 0))
		c.closeErr = c.raw.Close()
		if c.onClose != nil {
			c.onClose()
		}
	})
	return c.closeErr
}

func (c *conn) LocalAddr() net.Addr  { return c.local }
func (c *conn) RemoteAddr() net.Addr { return c.remote }

func (c *conn) SetDeadline(t time.Time) error {
	c.readDeadline.set(t)
	c.writeDeadline.set(t)
	return nil
}

func (c *conn) SetReadDeadline(t time.Time) error {
	c.readDeadline.set(t)
	return nil
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline.set(t)
	return nil
}

// deadline is a resettable timeout in the shape net.Conn needs: the channel
// from expired is closed once the deadline set last has passed.
type deadline struct {
	mu    sync.Mutex
	timer *time.Timer
	done  chan struct{}
}

func (d *deadline) expired() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done == nil {
		d.done = make(chan struct{})
	}
	return d.done
}

func (d *deadline) set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done == nil {
		d.done = make(chan struct{})
	}
	if d.timer != nil && !d.timer.Stop() {
		<-d.done // the timer fired; wait for it to finish closing done
	}
	d.timer = nil

	fired := false
	select {
	case <-d.done:
		fired = true
	default:
	}
	until := time.Until(t)
	if fired && (t.IsZero() || until > 0) {
		d.done = make(chan struct{})
	}
	switch {
	case t.IsZero():
	case until > 0:
		done := d.done
		d.timer = time.AfterFunc(until, func() { close(done) })
	case !fired:
		close(d.done)
	}
}
