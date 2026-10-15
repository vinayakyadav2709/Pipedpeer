package daemonapi

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/narpack"
)

// closureSendStall cuts a closure send that moved no bytes for this long. A
// send that keeps moving, however slowly, is left to finish: a 10-minute
// total cap failed a big closure on a slow link on every run, and every
// retry started over.
var closureSendStall = 60 * time.Second

// closureReplyWait bounds the wait for the peer's answer once every byte is
// sent; its import (and its own binary-cache fetch, capped at 30 s) runs then.
var closureReplyWait = 10 * time.Minute

// sendWatch cancels one closure send when its body stops moving, or when
// the peer sits on the finished body past closureReplyWait.
type sendWatch struct {
	cancel    context.CancelFunc
	stall     time.Duration
	replyWait time.Duration
	last      atomic.Int64 // unix nanos of the last byte read for the body
	bytes     atomic.Int64
	sentAt    atomic.Int64 // unix nanos once the whole body is written; 0 before
	mu        sync.Mutex
	why       string
	quit      chan struct{}
	stopOnce  sync.Once
}

func newSendWatch(cancel context.CancelFunc) *sendWatch {
	w := &sendWatch{cancel: cancel, stall: closureSendStall, replyWait: closureReplyWait, quit: make(chan struct{})}
	w.last.Store(time.Now().UnixNano())
	go w.run()
	return w
}

func (w *sendWatch) run() {
	t := time.NewTicker(min(w.stall/10, time.Second))
	defer t.Stop()
	for {
		select {
		case <-w.quit:
			return
		case <-t.C:
		}
		now := time.Now()
		if at := w.sentAt.Load(); at != 0 {
			if now.Sub(time.Unix(0, at)) > w.replyWait {
				w.fire(fmt.Sprintf("the peer did not answer within %s of receiving the whole closure", w.replyWait))
				return
			}
			continue
		}
		if idle := now.Sub(time.Unix(0, w.last.Load())); idle > w.stall {
			w.fire(fmt.Sprintf("closure send stalled: no bytes moved for %s (%s sent)",
				idle.Round(100*time.Millisecond), narpack.Human(w.bytes.Load())))
			return
		}
	}
}

func (w *sendWatch) fire(why string) {
	w.mu.Lock()
	w.why = why
	w.mu.Unlock()
	log.Warn().Msg(why + "; cutting it")
	w.cancel()
}

// cut says why the watch cancelled the send, or "" if it did not.
func (w *sendWatch) cut() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.why
}

func (w *sendWatch) sent() { w.sentAt.Store(time.Now().UnixNano()) }

func (w *sendWatch) stop() { w.stopOnce.Do(func() { close(w.quit) }) }

// reader counts every byte read from r as progress.
func (w *sendWatch) reader(r io.Reader) io.Reader { return watchedReader{r, w} }

type watchedReader struct {
	r io.Reader
	w *sendWatch
}

func (wr watchedReader) Read(p []byte) (int, error) {
	n, err := wr.r.Read(p)
	if n > 0 {
		wr.w.bytes.Add(int64(n))
		wr.w.last.Store(time.Now().UnixNano())
	}
	return n, err
}
