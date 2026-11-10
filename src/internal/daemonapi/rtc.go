package daemonapi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/rtctransport"
)

// EnableWebRTC makes this daemon answer WebRTC offers at
// rtctransport.SignalPath, behind the same token as every other route, and
// serve the API over the data channels they open. Call it before
// ListenAndServe.
func (s *Server) EnableWebRTC(cfg rtctransport.Config) {
	s.rtcListener = rtctransport.NewListener(cfg)
	s.buildRouter()
}

// WebRTCTransport sends daemon API calls over WebRTC data channels and
// everything else, and every call WebRTC cannot carry, over base. Offers are
// signalled over base, so a data channel is as authenticated as base is.
func WebRTCTransport(cfg rtctransport.Config, base http.RoundTripper) http.RoundTripper {
	cfg.Signal = base
	return &fallbackTransport{
		primary:  authtoken.Wrap(markDialFailures(rtctransport.NewHTTPTransport(rtctransport.NewDialer(cfg)))),
		fallback: base,
	}
}

// overWebRTC reports whether a request is a daemon API call that WebRTC
// should carry. Anything else may be for a host that is not a daemon at all.
func overWebRTC(req *http.Request) bool {
	if req.URL.Scheme != "http" {
		return false
	}
	p := req.URL.Path
	return p == "/health" || strings.HasPrefix(p, "/v1/")
}

// dialError marks a request that failed before a connection for it existed.
// Nothing of it was written, so sending it another way cannot repeat it.
type dialError struct{ err error }

func (e *dialError) Error() string { return e.err.Error() }
func (e *dialError) Unwrap() error { return e.err }

// markDialFailures makes t report a failed dial as a *dialError.
func markDialFailures(t *http.Transport) *http.Transport {
	dial := t.DialContext
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, &dialError{err}
		}
		return conn, nil
	}
	return t
}

// fallbackTransport sends a request over fallback when primary fails and a
// second send cannot repeat an effect: the first never left (its dial failed),
// or the method changes nothing on the peer. A POST lost after it was written
// may already have been acted on, so its error goes back to the caller.
type fallbackTransport struct {
	primary, fallback http.RoundTripper
}

func (t *fallbackTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !overWebRTC(req) {
		return t.fallback.RoundTrip(req)
	}
	resp, err := t.primary.RoundTrip(req)
	if err == nil {
		return resp, nil
	}
	var unsent *dialError
	if !errors.As(err, &unsent) && !safeToRepeat(req.Method) {
		return nil, err
	}
	retry := req.Clone(req.Context())
	if req.Body != nil && req.Body != http.NoBody {
		if req.GetBody == nil {
			return nil, err
		}
		body, gerr := req.GetBody()
		if gerr != nil {
			return nil, err
		}
		retry.Body = body
	}
	return t.fallback.RoundTrip(retry)
}

// safeToRepeat reports whether a second send of the method leaves a peer as
// one send would. No peer endpoint behind another method promises that.
func safeToRepeat(method string) bool {
	return method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions
}

// maxPendingOffers is how many WebRTC offers may be in negotiation at once.
// Each one builds a peer connection and holds it while ICE candidates are
// gathered; a LAN's worth of peers reconnecting together stays well under it.
const maxPendingOffers = 16

// limitPending lets at most limit requests into next at a time and answers
// the rest 429.
func limitPending(limit int, next http.Handler) http.Handler {
	slots := make(chan struct{}, limit)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			next.ServeHTTP(w, r)
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many offers in negotiation", http.StatusTooManyRequests)
		}
	})
}

// rtcRemoteAddr names the far end of a data channel. It is not an IP address:
// an offer signalled over loopback (a co-located daemon, a direct-link
// forwarder) must not give the channels it opens the trust loopback callers
// get.
func rtcRemoteAddr(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(r.Context())
		r.RemoteAddr = "webrtc:" + r.RemoteAddr
		next.ServeHTTP(w, r)
	})
}
