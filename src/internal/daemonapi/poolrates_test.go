package daemonapi

import (
	"math"
	"testing"
	"time"
)

// TestFitSeparatesRoundTripFromPerItemCost is the measurement the share split
// rests on. Treating a peer's whole elapsed time as per-item work reads a slow
// link as a slow machine, which is the difference between "send it less" and
// "do not send to it at all".
func TestFitSeparatesRoundTripFromPerItemCost(t *testing.T) {
	m := newRateModel()
	// A peer with a 2s round trip that then does 100 items/second.
	for _, items := range []int{100, 200, 400, 800} {
		m.observe("peer", items, time.Duration(float64(time.Second)*(2+float64(items)/100)))
	}
	rate, setup, ok := m.fit("peer")
	if !ok {
		t.Fatal("no fit from four observations")
	}
	if math.Abs(rate-100) > 1 {
		t.Errorf("rate = %.2f items/s, want 100", rate)
	}
	if math.Abs(setup-2) > 0.05 {
		t.Errorf("setup = %.3fs, want 2s — the round trip was folded into the "+
			"per-item cost, which reads a slow link as a slow machine", setup)
	}
}

// TestFitFallsBackWhenObservationsCannotSeparateTheTwo: with every part the
// same size there is no spread to fit a line through, and inventing an
// intercept from noise is worse than admitting the per-item estimate is
// approximate.
func TestFitFallsBackWhenObservationsCannotSeparateTheTwo(t *testing.T) {
	m := newRateModel()
	for i := 0; i < 4; i++ {
		m.observe("peer", 100, 3*time.Second) // always 100 items in 3s
	}
	rate, setup, ok := m.fit("peer")
	if !ok {
		t.Fatal("no estimate at all from four identical observations")
	}
	if setup != 0 {
		t.Errorf("setup = %v, want 0: identical sizes carry no information about "+
			"the intercept", setup)
	}
	if math.Abs(rate-100.0/3.0) > 0.1 {
		t.Errorf("rate = %.2f, want %.2f (items over total elapsed)", rate, 100.0/3.0)
	}
}

// TestFitIgnoresAnImpossibleIntercept. Noise can produce a best-fit line that
// crosses below zero, which would mean a negative round trip; clamping beats
// both believing it and discarding a usable measurement.
func TestFitIgnoresAnImpossibleIntercept(t *testing.T) {
	_, setup, ok := fitLine([]obs{{100, 1.2}, {200, 2.6}, {400, 5.4}})
	if !ok {
		t.Fatal("no fit")
	}
	if setup < 0 {
		t.Errorf("setup = %v; a negative round trip is not a measurement", setup)
	}
}

// TestObservationsExpire keeps the model current. A peer that was fast ten
// minutes ago and is now running someone else's job must not keep drawing
// work on the strength of its old numbers.
func TestObservationsExpire(t *testing.T) {
	m := newRateModel()
	for i := 0; i < obsWindow; i++ {
		m.observe("peer", 100, 1*time.Second) // 100 items/s
	}
	if rate, _, _ := m.fit("peer"); math.Abs(rate-100) > 1 {
		t.Fatalf("setup: rate = %.1f, want 100", rate)
	}
	// The machine gets busy: same sizes, ten times slower.
	for i := 0; i < obsWindow; i++ {
		m.observe("peer", 100, 10*time.Second)
	}
	rate, _, _ := m.fit("peer")
	if math.Abs(rate-10) > 1 {
		t.Errorf("rate = %.1f after the peer slowed by 10x, want about 10 — old "+
			"observations are still being counted", rate)
	}
}

// TestUnmeasuredPeerStillGetsAChance guards a self-fulfilling failure: a
// pessimistic prior sizes an unknown peer's share below anything worth
// sending, so nothing is sent, so nothing is ever measured, so the peer stays
// unknown forever. That is exactly what a "assume one core" default did.
func TestUnmeasuredPeerStillGetsAChance(t *testing.T) {
	m := newRateModel()
	devices := m.devices([]string{"newpeer"}, func(string) int { return 0 }, 20, true)
	if len(devices) != 2 {
		t.Fatalf("got %d devices, want local plus the peer", len(devices))
	}
	var peer bool
	for _, d := range devices {
		if d.ID == "newpeer" {
			peer = true
			if d.Rate <= 0 {
				t.Errorf("unmeasured peer given rate %v; it will never be tried", d.Rate)
			}
		}
	}
	if !peer {
		t.Error("unmeasured peer missing from the device list entirely")
	}
}

// TestMeasurementOverridesThePrior: once a peer has been timed, the guess
// stops mattering.
func TestMeasurementOverridesThePrior(t *testing.T) {
	m := newRateModel()
	// Advertises 64 cores, actually manages 5 items/second.
	for _, items := range []int{50, 100} {
		m.observe("liar", items, time.Duration(float64(time.Second)*float64(items)/5))
	}
	devices := m.devices([]string{"liar"}, func(string) int { return 64 }, 20, false)
	if len(devices) != 1 {
		t.Fatalf("got %d devices", len(devices))
	}
	if devices[0].Rate > 10 {
		t.Errorf("rate = %.1f; the 64-core advertisement is still being believed "+
			"over the measurement", devices[0].Rate)
	}
}
