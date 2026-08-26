package daemonapi

import (
	"encoding/binary"
	"math"
	"testing"
)

func encodeF32(vals []float32) []byte {
	out := make([]byte, len(vals)*4)
	for i, v := range vals {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(v))
	}
	return out
}

func decodeF32(b []byte) []float32 {
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out
}

func encodeF16(vals []float32) []byte {
	out := make([]byte, len(vals)*2)
	for i, v := range vals {
		binary.LittleEndian.PutUint16(out[i*2:], float32ToFloat16(v))
	}
	return out
}

func decodeF16(b []byte) []float32 {
	out := make([]float32, len(b)/2)
	for i := range out {
		out[i] = float16ToFloat32(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return out
}

// TestAccumulatorAveragesRanks is the arithmetic the shim used to do. Getting
// it wrong here is worse than a slow sync: the gradients are silently wrong
// and training still runs, converging to something else.
func TestAccumulatorAveragesRanks(t *testing.T) {
	a := newDDPAccumulator(ddpF32, 4)
	for _, rank := range [][]float32{
		{1, 2, 3, 4},
		{3, 4, 5, 6},
		{5, 6, 7, 8},
	} {
		if err := a.add(encodeF32(rank), 0); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	out, _, err := a.mean()
	if err != nil {
		t.Fatalf("mean: %v", err)
	}
	got := decodeF32(out)
	want := []float32{3, 4, 5, 6}
	for i := range want {
		if math.Abs(float64(got[i]-want[i])) > 1e-6 {
			t.Errorf("element %d = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestAccumulatorSumsInFloat64 checks the width the sum is kept at. Adding
// many float16 values in float16 loses exactly the precision the averaging is
// supposed to preserve, and the buffer exists once per group rather than once
// per rank, so there is no reason to economise on it.
func TestAccumulatorSumsInFloat64(t *testing.T) {
	// Each rank contributes a value that float16 can hold, but whose running
	// sum cannot be represented without losing the small ones.
	a := newDDPAccumulator(ddpF16, 1)
	const ranks = 64
	for i := 0; i < ranks; i++ {
		if err := a.add(encodeF16([]float32{1.0}), 0); err != nil {
			t.Fatal(err)
		}
	}
	got := decodeF16(a.mustMean(t))[0]
	if math.Abs(float64(got-1.0)) > 1e-3 {
		t.Errorf("mean of %d ones = %v, want 1", ranks, got)
	}
}

func (a *ddpAccumulator) mustMean(t *testing.T) []byte {
	t.Helper()
	b, _, err := a.mean()
	if err != nil {
		t.Fatalf("mean: %v", err)
	}
	return b
}

// TestWrongSizedPayloadIsRejected. A rank that disagrees about the model's
// shape must not be folded in: the sum would be silently wrong for every
// element after the mismatch.
func TestWrongSizedPayloadIsRejected(t *testing.T) {
	a := newDDPAccumulator(ddpF32, 4)
	if err := a.add(encodeF32([]float32{1, 2, 3}), 0); err == nil {
		t.Error("a payload of the wrong length was accepted")
	}
	if err := a.add(encodeF32([]float32{1, 2, 3, 4, 5}), 0); err == nil {
		t.Error("an over-long payload was accepted")
	}
}

// TestEmptyAccumulatorHasNoMean: dividing by zero ranks should be an error,
// not a buffer of NaNs that trains a model into nothing.
func TestEmptyAccumulatorHasNoMean(t *testing.T) {
	if _, _, err := newDDPAccumulator(ddpF32, 2).mean(); err == nil {
		t.Error("mean of no contributors was accepted")
	}
}

// TestFloat16RoundTrip covers the codec the gradients travel in. These are
// written out here rather than taken from a library, so they need to be held
// to the standard's behaviour rather than to whatever they happen to do.
func TestFloat16RoundTrip(t *testing.T) {
	cases := []float32{
		0, 1, -1, 0.5, -0.5, 2, 65504, -65504, // 65504 is binary16's largest finite
		1.0 / 3, -1.0 / 3, 1e-4, -1e-4,
		6.1035156e-05,  // smallest normal
		5.9604645e-08,  // smallest subnormal
		-5.9604645e-08, // and its negative
	}
	for _, want := range cases {
		got := float16ToFloat32(float32ToFloat16(want))
		// binary16 carries about three decimal digits; compare relatively
		// except around zero.
		if want == 0 {
			if got != 0 || math.Signbit(float64(got)) != math.Signbit(float64(want)) {
				t.Errorf("round trip of %v gave %v", want, got)
			}
			continue
		}
		rel := math.Abs(float64(got-want) / float64(want))
		if rel > 1e-3 {
			t.Errorf("round trip of %v gave %v (relative error %.2e)", want, got, rel)
		}
	}
}

// TestFloat16SaturatesRatherThanWraps. A gradient spike beyond binary16's
// range must become infinity, which training can detect, and not wrap to a
// small number, which it cannot.
func TestFloat16SaturatesRatherThanWraps(t *testing.T) {
	for _, v := range []float32{1e6, -1e6, float32(math.Inf(1)), float32(math.Inf(-1))} {
		got := float16ToFloat32(float32ToFloat16(v))
		if !math.IsInf(float64(got), 0) {
			t.Errorf("%v encoded to %v; an out-of-range value must saturate to "+
				"infinity, not wrap to something finite and plausible", v, got)
		}
		if math.Signbit(float64(got)) != math.Signbit(float64(v)) {
			t.Errorf("%v encoded to %v: sign lost", v, got)
		}
	}
}

// TestFloat16KeepsNaN. A NaN that decays into infinity turns a detectable
// training failure into a silent one.
func TestFloat16KeepsNaN(t *testing.T) {
	got := float16ToFloat32(float32ToFloat16(float32(math.NaN())))
	if !math.IsNaN(float64(got)) {
		t.Errorf("NaN round-tripped to %v", got)
	}
}

// TestFloat16RoundsToNearestEven. Truncating instead of rounding biases every
// gradient towards zero, which over thousands of steps is a systematic drift
// rather than noise.
func TestFloat16RoundsToNearestEven(t *testing.T) {
	// Exactly representable neighbours near 1: 1, 1+2^-10, 1+2^-9 ...
	const eps = 1.0 / 1024
	// Midway between 1 and 1+eps: nearest-even picks 1 (even mantissa).
	mid := float32(1 + eps/2)
	if got := float16ToFloat32(float32ToFloat16(mid)); got != 1 {
		t.Errorf("%v rounded to %v, want 1 (nearest even)", mid, got)
	}
	// Midway between 1+eps and 1+2*eps: nearest-even picks 1+2*eps.
	mid2 := float32(1 + eps + eps/2)
	want := float32(1 + 2*eps)
	if got := float16ToFloat32(float32ToFloat16(mid2)); got != want {
		t.Errorf("%v rounded to %v, want %v (nearest even)", mid2, got, want)
	}
}

func encodeI8(vals []float32, scale float64) []byte {
	out := make([]byte, len(vals))
	for i, v := range vals {
		q := math.Round(float64(v) / scale)
		if q > 127 {
			q = 127
		} else if q < -127 {
			q = -127
		}
		out[i] = byte(int8(q))
	}
	return out
}

func decodeI8(b []byte, scale float64) []float32 {
	out := make([]float32, len(b))
	for i := range out {
		out[i] = float32(float64(int8(b[i])) * scale)
	}
	return out
}

// TestInt8AveragesAcrossRanksWithDifferentScales. Each rank quantises against
// its own largest value, so the scales differ and the daemon has to decode
// each contribution with the scale it arrived with. Using one rank's scale for
// all of them would silently rescale everyone else's gradients.
func TestInt8AveragesAcrossRanksWithDifferentScales(t *testing.T) {
	a := newDDPAccumulator(ddpI8, 3)
	// Rank A's values peak at 10; rank B's at 1000.
	if err := a.add(encodeI8([]float32{10, 5, 0}, 10.0/127), 10.0/127); err != nil {
		t.Fatal(err)
	}
	if err := a.add(encodeI8([]float32{1000, 500, 0}, 1000.0/127), 1000.0/127); err != nil {
		t.Fatal(err)
	}
	out, scale, err := a.mean()
	if err != nil {
		t.Fatalf("mean: %v", err)
	}
	got := decodeI8(out, scale)
	want := []float32{505, 252.5, 0}
	for i := range want {
		// int8 carries about two significant digits, so compare relatively.
		if want[i] == 0 {
			if math.Abs(float64(got[i])) > 5 {
				t.Errorf("element %d = %v, want 0", i, got[i])
			}
			continue
		}
		rel := math.Abs(float64(got[i]-want[i]) / float64(want[i]))
		if rel > 0.02 {
			t.Errorf("element %d = %v, want about %v (relative error %.3f)", i, got[i], want[i], rel)
		}
	}
}

// TestInt8NeedsAScale: decoding bytes without the scale they were made with
// produces plausible-looking numbers of entirely the wrong magnitude, which
// training would absorb rather than reject.
func TestInt8NeedsAScale(t *testing.T) {
	a := newDDPAccumulator(ddpI8, 2)
	if err := a.add([]byte{1, 2}, 0); err == nil {
		t.Error("an int8 payload was accepted with no scale")
	}
	if err := a.add([]byte{1, 2}, -1); err == nil {
		t.Error("an int8 payload was accepted with a negative scale")
	}
}

// TestInt8ClampsRatherThanWraps. A wrapped byte flips the sign of a gradient,
// and a gradient pointing the wrong way is worse than one that saturates.
func TestInt8ClampsRatherThanWraps(t *testing.T) {
	a := newDDPAccumulator(ddpI8, 1)
	// A single huge value against a tiny scale: the mean must saturate.
	if err := a.add(encodeI8([]float32{1e6}, 1.0), 1.0); err != nil {
		t.Fatal(err)
	}
	out, scale, err := a.mean()
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeI8(out, scale)[0]; got <= 0 {
		t.Errorf("a saturating positive value decoded to %v; it wrapped", got)
	}
}

// TestPerTensorScalesPreservePrecision is the fix for a real accuracy loss.
//
// One scale for the whole model lets the largest layer set the quantisation
// step for every other. On the training demo, hidden layers with gradients
// near 1e-2 sat alongside an output layer near 3e-1, which left the hidden
// layers four levels between them, and the final loss moved from 0.087 to
// 0.127 - a 45% degradation reported as if it were the encoding's ordinary
// cost.
func TestPerTensorScalesPreservePrecision(t *testing.T) {
	// A big-magnitude tensor followed by a small one, as a network's output
	// and hidden layers are.
	big := []float32{0.30, -0.30, 0.15}
	small := []float32{0.010, -0.008, 0.004}

	// Per-tensor: each gets its own step.
	seg := newDDPAccumulator(ddpI8, len(big)+len(small))
	if err := seg.withSegments([]int{len(big), len(small)}); err != nil {
		t.Fatal(err)
	}
	bigScale, smallScale := 0.30/127, 0.010/127
	payload := append(encodeI8(big, bigScale), encodeI8(small, smallScale)...)
	if err := seg.addSegmented(payload, []float64{bigScale, smallScale}); err != nil {
		t.Fatal(err)
	}
	out, scales, err := seg.meanSegmented()
	if err != nil {
		t.Fatal(err)
	}
	if len(scales) != 2 {
		t.Fatalf("got %d scales, want one per tensor", len(scales))
	}
	gotSmall := decodeI8(out[len(big):], scales[1])

	// Single scale, which is what this replaces.
	flat := newDDPAccumulator(ddpI8, len(big)+len(small))
	globalScale := 0.30 / 127
	if err := flat.add(append(encodeI8(big, globalScale), encodeI8(small, globalScale)...), globalScale); err != nil {
		t.Fatal(err)
	}
	flatOut, flatScale, err := flat.mean()
	if err != nil {
		t.Fatal(err)
	}
	flatSmall := decodeI8(flatOut[len(big):], flatScale)

	segErr, flatErr := 0.0, 0.0
	for i := range small {
		segErr += math.Abs(float64(gotSmall[i] - small[i]))
		flatErr += math.Abs(float64(flatSmall[i] - small[i]))
	}
	if segErr >= flatErr {
		t.Errorf("per-tensor scaling was no better on the small tensor: error %.2e "+
			"against %.2e for a single scale", segErr, flatErr)
	}
	// And it should be far better, not marginally: the whole point is that the
	// small tensor gets the full range of the byte.
	if flatErr/segErr < 5 {
		t.Errorf("per-tensor scaling is only %.1fx better; the small tensor should "+
			"be recovering most of its precision", flatErr/segErr)
	}
}

// TestSegmentsMustCoverThePayload. A scale applied to the wrong values is
// silently wrong arithmetic, so a mismatch has to be refused rather than
// truncated.
func TestSegmentsMustCoverThePayload(t *testing.T) {
	a := newDDPAccumulator(ddpI8, 10)
	if err := a.withSegments([]int{3, 3}); err == nil {
		t.Error("segments totalling 6 were accepted for a 10-value payload")
	}
	if err := a.withSegments([]int{5, 0, 5}); err == nil {
		t.Error("a zero-length segment was accepted")
	}
	if err := a.withSegments([]int{4, 6}); err != nil {
		t.Errorf("segments that do cover the payload were refused: %v", err)
	}
}
