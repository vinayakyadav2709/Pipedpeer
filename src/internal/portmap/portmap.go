// Package portmap asks the router for a port instead of tricking it.
//
// Hole punching works by both sides sending at once so each router accepts
// the other's packets. It fails when a router hands out a different external
// port per destination, because then there is no address to tell the peer
// about - measured on this project's own laptop, which on a phone tether was
// seen as :38471 by one probe and :27486 by the next.
//
// Port mapping sidesteps all of that. "Forward external port N to me for the
// next two hours" gets a stable address that can be published, needs no
// simultaneity, and works regardless of what the mapping behaviour is,
// because the dynamic mapping is no longer involved. When a router grants
// one, it is strictly the better answer.
//
// Three protocols do the same job and routers vary in which they enable:
// PCP (RFC 6887), its predecessor NAT-PMP (RFC 6886) - both on UDP 5351 -
// and UPnP-IGD, which is XML over HTTP found by multicast. All three are
// tried.
//
// The limit worth knowing: this cannot see past a second layer of NAT. On a
// phone tether the router that answers is the phone, and mapping a port on
// it achieves nothing, because the carrier's NAT sits above and was never
// asked. A mapping whose external address is itself private or carrier-grade
// says so rather than being published as though the world could reach it.
//
// Security: a mapping exposes one UDP port, and the only thing behind it is
// a QUIC listener that authenticates every peer against a pinned identity
// before it will do anything. Nothing unauthenticated is admitted by opening
// it.
package portmap

import (
	"context"
	"fmt"
	"net/netip"
	"sync"
	"time"
)

// Kind is which protocol produced a mapping, for the user-facing report.
type Kind string

const (
	KindPCP    Kind = "pcp"
	KindNATPMP Kind = "nat-pmp"
	KindUPnP   Kind = "upnp"
	KindNone   Kind = "none"
)

// Mapping is a port the router has agreed to forward.
type Mapping struct {
	// External is the address to publish - subject to Public below.
	External netip.AddrPort
	// Internal is the local port being forwarded.
	Internal uint16
	// Lifetime is what the router granted, which is often less than was
	// asked for. Renewal is scheduled from this, not from the request.
	Lifetime time.Duration
	// Kind names the protocol that worked.
	Kind Kind
	// Public is false when the mapping is real but its external address is
	// not reachable from the internet - the double-NAT case. Such a mapping
	// is still worth having as one more candidate to try on the local
	// network, and must never be advertised as a public address.
	Public bool
}

// String is the one-line form for logs and `pipedpeer nodes`.
func (m Mapping) String() string {
	if !m.External.IsValid() {
		return "none"
	}
	s := fmt.Sprintf("%s via %s", m.External, m.Kind)
	if !m.Public {
		s += " (behind another NAT; not publicly reachable)"
	}
	return s
}

// defaultLifetime is what to ask for. Two hours is long enough that renewal
// is rare and short enough that a mapping left behind by a crashed daemon
// expires the same afternoon.
const defaultLifetime = 2 * time.Hour

// Map asks the router to forward internal to this machine, trying each
// protocol in turn.
//
// The error returned when nothing works is the ordinary case, not a fault:
// plenty of routers have port mapping switched off, and the caller's answer
// to that is to fall back on punching rather than to fail.
func Map(ctx context.Context, internal uint16) (Mapping, error) {
	return mapWith(ctx, internal, defaultLifetime)
}

func mapWith(ctx context.Context, internal uint16, lifetime time.Duration) (Mapping, error) {
	var errs []error

	if gw, err := defaultGateway(); err == nil {
		if ext, granted, err := pcpMap(ctx, gw, internal, lifetime); err == nil {
			return finish(ext, internal, granted, lifetime, KindPCP), nil
		} else {
			errs = append(errs, fmt.Errorf("pcp: %w", err))
		}
		if ext, granted, err := natpmpMap(ctx, gw, internal, lifetime); err == nil {
			return finish(ext, internal, granted, lifetime, KindNATPMP), nil
		} else {
			errs = append(errs, fmt.Errorf("nat-pmp: %w", err))
		}
	} else {
		errs = append(errs, fmt.Errorf("gateway: %w", err))
	}

	if ext, granted, err := upnpMap(ctx, internal, lifetime, "pipedpeer"); err == nil {
		return finish(ext, internal, granted, lifetime, KindUPnP), nil
	} else {
		errs = append(errs, fmt.Errorf("upnp: %w", err))
	}

	return Mapping{Kind: KindNone}, fmt.Errorf("no port mapping available: %w", joinErrs(errs))
}

func finish(ext netip.AddrPort, internal uint16, granted, asked time.Duration, kind Kind) Mapping {
	if granted <= 0 {
		granted = asked
	}
	return Mapping{
		External: ext,
		Internal: internal,
		Lifetime: granted,
		Kind:     kind,
		Public:   isPublic(ext.Addr()),
	}
}

// isPublic reports whether an address is one the internet can reach.
//
// The check that catches double NAT. A router behind a carrier's NAT will
// happily grant a mapping and report an external address in 100.64.0.0/10
// (RFC 6598, the carrier-grade range) or in ordinary private space; treating
// that as publishable is how a peer ends up being told to connect to an
// address that means something entirely different on its own network.
func isPublic(a netip.Addr) bool {
	if !a.IsValid() {
		return false
	}
	a = a.Unmap()
	if a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() ||
		a.IsUnspecified() || a.IsMulticast() {
		return false
	}
	// 100.64.0.0/10 - carrier-grade NAT. Not covered by IsPrivate.
	if a.Is4() {
		b := a.As4()
		if b[0] == 100 && b[1] >= 64 && b[1] <= 127 {
			return false
		}
	}
	return true
}

func joinErrs(errs []error) error {
	if len(errs) == 0 {
		return fmt.Errorf("no protocol was tried")
	}
	msg := ""
	for i, e := range errs {
		if i > 0 {
			msg += "; "
		}
		msg += e.Error()
	}
	return fmt.Errorf("%s", msg)
}

// Keeper holds a mapping open, renewing before it expires.
//
// Mappings are leases and routers do forget them; a peer list that names an
// address the router stopped forwarding twenty minutes ago is worse than one
// that never named it, because nothing reports an error - packets simply
// stop arriving.
type Keeper struct {
	mu      sync.Mutex
	current Mapping
	stop    context.CancelFunc
	log     func(string, ...any)
}

// NewKeeper maps the port and keeps it mapped until Close.
//
// It returns successfully even when no mapping could be made: the zero
// Mapping with KindNone is a legitimate state that the caller reports and
// works around, and refusing to start a daemon because a router will not
// forward a port would be absurd.
func NewKeeper(ctx context.Context, internal uint16, log func(string, ...any)) *Keeper {
	if log == nil {
		log = func(string, ...any) {}
	}
	k := &Keeper{log: log}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	k.stop = cancel

	first, err := Map(ctx, internal)
	if err != nil {
		k.log("no port mapping: %v", err)
	}
	k.set(first)

	go k.renew(runCtx, internal)
	return k
}

// Current is the mapping as it stands, or KindNone if there is not one.
func (k *Keeper) Current() Mapping {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.current
}

func (k *Keeper) set(m Mapping) {
	k.mu.Lock()
	k.current = m
	k.mu.Unlock()
}

// renewInterval is when to renew a lease of the given length.
//
// Half-life: one missed renewal still leaves as long again to try before the
// mapping is gone. Floored, because a router that grants a very short lease
// would otherwise have this spinning.
func renewInterval(lifetime time.Duration) time.Duration {
	d := lifetime / 2
	if d < 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func (k *Keeper) renew(ctx context.Context, internal uint16) {
	for {
		cur := k.Current()
		wait := renewInterval(cur.Lifetime)
		if cur.Kind == KindNone {
			// Nothing was granted. Retry occasionally rather than never: a
			// laptop that moves from a tether to a home router should get a
			// mapping without being restarted.
			wait = 5 * time.Minute
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		attemptCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		m, err := Map(attemptCtx, internal)
		cancel()
		if err != nil {
			k.log("port mapping renewal failed: %v", err)
			// Keep announcing the old one until it actually lapses; it may
			// well still be live and this may have been a blip.
			continue
		}
		if m.External != cur.External {
			k.log("port mapping changed: %s -> %s", cur.String(), m.String())
		}
		k.set(m)
	}
}

// Close stops renewing and gives the port back.
func (k *Keeper) Close() {
	if k.stop != nil {
		k.stop()
	}
	cur := k.Current()
	if cur.Kind == KindNone || cur.Kind == KindUPnP {
		if cur.Kind == KindUPnP {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = upnpUnmap(ctx, cur.Internal)
		}
		return
	}
	// PCP and NAT-PMP delete a mapping by asking for it again with a
	// lifetime of zero.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if gw, err := defaultGateway(); err == nil {
		switch cur.Kind {
		case KindPCP:
			_, _, _ = pcpMap(ctx, gw, cur.Internal, 0)
		case KindNATPMP:
			_, _, _ = natpmpMap(ctx, gw, cur.Internal, 0)
		}
	}
}
