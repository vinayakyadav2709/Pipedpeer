package daemonapi

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Server-side averaging for DDP.
//
// The blackboard's first form was deliberately opaque: ranks posted pickled
// payloads and the daemon handed every rank the whole set back, averaging in
// the shim. That is a clean separation and it is expensive in three ways at
// once, all of which scale with the number of ranks.
//
// Measured on two machines over a home link, an 800k-parameter model: each
// rank sent 1.5 MiB per step and received 3.0 MiB, because the reply carried
// every rank's blob. 88 steps cost 60s of sync against 2.9s of compute for the
// same job on one machine - a 21x pessimisation, on the feature this project
// exists for. The download is N times the model, the lead daemon holds N full
// models at once, and every rank unpickles N of them and adds them up.
//
// Averaging here makes the reply one model instead of N, holds one accumulator
// instead of N blobs, and leaves the shim nothing to do but read the result.
// The daemon has to understand the payload for that, so the format below is
// explicit: a header naming the dtype and the element count, and raw
// little-endian values after it. No pickle - which also takes Python's
// serialisation out of the step.

// ddpDType is the wire element type. Only what actually crosses: gradients go
// as float16 by default and float32 when compression is off, and weight
// broadcasts stay at their own width.
type ddpDType string

const (
	ddpF16 ddpDType = "float16"
	ddpF32 ddpDType = "float32"
	ddpF64 ddpDType = "float64"
	// ddpI8 is fp16 halved again: values are scaled into a signed byte, with
	// the scale carried in the header. Halving the bytes halves the sync on a
	// link where sync is the whole cost, and the error it introduces is not
	// lost - the shim carries it forward and folds it into the next step, so
	// what is dropped is delayed rather than discarded.
	ddpI8 ddpDType = "int8"
)

func (d ddpDType) size() int {
	switch d {
	case ddpF16:
		return 2
	case ddpF32:
		return 4
	case ddpF64:
		return 8
	case ddpI8:
		return 1
	}
	return 0
}

// ddpAccumulator sums ranks' payloads as they arrive, so the lead daemon holds
// one buffer rather than one payload per rank. On a large model with a wide
// ring that is the difference between a working node and an OOM.
type ddpAccumulator struct {
	dtype ddpDType
	// segments are the tensor boundaries, for int8 where each tensor carries
	// its own scale.
	//
	// One scale for the whole model is the obvious encoding and it is badly
	// wrong: a network's layers differ in gradient magnitude by an order of
	// magnitude or more, so the largest layer sets the step size and the rest
	// quantise to a handful of levels. Measured on the demo - an output layer
	// peaking near 3e-1 alongside hidden layers near 1e-2 leaves those hidden
	// layers with four levels between them, and the final loss moved from
	// 0.087 to 0.127.
	segments []int
	// sum is always float64 regardless of the wire width. Adding N float16
	// values in float16 loses precision the averaging is supposed to
	// preserve, and this buffer exists once per group, not once per rank.
	sum   []float64
	count int
	// weight is the total sample count folded in, and the divisor the mean
	// uses. With equal batches every rank weighs 1 and this is the rank
	// count, exactly as before.
	//
	// Unequal batches make the plain average wrong rather than merely
	// imprecise: a rank training on twice the data has computed a gradient
	// over twice the samples, and averaging the two per-rank means as equals
	// gives the small shard twice the influence per sample it deserves. The
	// combined gradient is sum(n_i * g_i) / sum(n_i).
	weight float64
}

func newDDPAccumulator(dtype ddpDType, n int) *ddpAccumulator {
	return &ddpAccumulator{dtype: dtype, sum: make([]float64, n)}
}

// withSegments records the tensor boundaries a payload is divided into. The
// lengths must add up to the element count, or a scale would be applied to the
// wrong values.
func (a *ddpAccumulator) withSegments(counts []int) error {
	total := 0
	for _, c := range counts {
		if c <= 0 {
			return fmt.Errorf("segment length %d is not positive", c)
		}
		total += c
	}
	if total != len(a.sum) {
		return fmt.Errorf("segments total %d values, payload declares %d", total, len(a.sum))
	}
	a.segments = counts
	return nil
}

// add folds one rank's payload into the running sum. scale is meaningful only
// for int8, where each rank picks its own from its own values.
func (a *ddpAccumulator) add(payload []byte, scale float64) error {
	return a.addSegmented(payload, []float64{scale}, 1)
}

// addSegmented folds one rank's payload in, decoding each tensor with its own
// scale, and weighting it by the number of samples that produced it.
//
// weight <= 0 is read as 1: a rank that does not say how many samples it
// trained on is one running the equal-batch code, and equal weights reproduce
// the plain average exactly.
func (a *ddpAccumulator) addSegmented(payload []byte, scales []float64, weight float64) error {
	if weight <= 0 {
		weight = 1
	}
	if a.dtype == ddpI8 && len(a.segments) > 1 {
		if len(scales) != len(a.segments) {
			return fmt.Errorf("%d scale(s) for %d segment(s)", len(scales), len(a.segments))
		}
		want := len(a.sum)
		if len(payload) != want {
			return fmt.Errorf("payload is %d bytes, want %d int8 values", len(payload), want)
		}
		off := 0
		for i, n := range a.segments {
			sc := scales[i]
			if sc <= 0 {
				return fmt.Errorf("segment %d has a non-positive scale", i)
			}
			for j := 0; j < n; j++ {
				a.sum[off+j] += float64(int8(payload[off+j])) * sc * weight
			}
			off += n
		}
		a.count++
		a.weight += weight
		return nil
	}
	return a.addFlat(payload, scales[0], weight)
}

func (a *ddpAccumulator) addFlat(payload []byte, scale float64, weight float64) error {
	if weight <= 0 {
		weight = 1
	}
	want := len(a.sum) * a.dtype.size()
	if len(payload) != want {
		return fmt.Errorf("payload is %d bytes, want %d for %d %s values",
			len(payload), want, len(a.sum), a.dtype)
	}
	switch a.dtype {
	case ddpF16:
		for i := range a.sum {
			a.sum[i] += float64(float16ToFloat32(binary.LittleEndian.Uint16(payload[i*2:]))) * weight
		}
	case ddpF32:
		for i := range a.sum {
			a.sum[i] += float64(math.Float32frombits(binary.LittleEndian.Uint32(payload[i*4:]))) * weight
		}
	case ddpF64:
		for i := range a.sum {
			a.sum[i] += math.Float64frombits(binary.LittleEndian.Uint64(payload[i*8:])) * weight
		}
	case ddpI8:
		if scale <= 0 {
			return fmt.Errorf("int8 payload needs a positive scale")
		}
		for i := range a.sum {
			a.sum[i] += float64(int8(payload[i])) * scale * weight
		}
	default:
		return fmt.Errorf("unsupported dtype %q", a.dtype)
	}
	a.count++
	a.weight += weight
	return nil
}

// meanSegmented is mean with a scale per tensor.
func (a *ddpAccumulator) meanSegmented() ([]byte, []float64, error) {
	if a.dtype != ddpI8 || len(a.segments) <= 1 {
		b, sc, err := a.mean()
		return b, []float64{sc}, err
	}
	if a.count == 0 {
		return nil, nil, fmt.Errorf("no ranks contributed")
	}
	inv := 1 / a.weight
	out := make([]byte, len(a.sum))
	scales := make([]float64, len(a.segments))

	off := 0
	for i, n := range a.segments {
		peak := 0.0
		for j := 0; j < n; j++ {
			if m := math.Abs(a.sum[off+j] * inv); m > peak {
				peak = m
			}
		}
		sc := 1.0
		if peak > 0 {
			sc = peak / 127
		}
		scales[i] = sc
		for j := 0; j < n; j++ {
			q := math.Round(a.sum[off+j] * inv / sc)
			if q > 127 {
				q = 127
			} else if q < -127 {
				q = -127
			}
			out[off+j] = byte(int8(q))
		}
		off += n
	}
	return out, scales, nil
}

// mean divides by the number of contributors and encodes at the wire width,
// returning the scale when the width needs one.
//
// Every rank gets these identical bytes, so quantising the reply adds noise to
// the update without letting the ranks drift apart - they still hold exactly
// the same model afterwards, which is the property that matters.
func (a *ddpAccumulator) mean() ([]byte, float64, error) {
	if a.count == 0 {
		return nil, 0, fmt.Errorf("no ranks contributed")
	}
	inv := 1 / a.weight
	out := make([]byte, len(a.sum)*a.dtype.size())
	switch a.dtype {
	case ddpF16:
		for i, v := range a.sum {
			binary.LittleEndian.PutUint16(out[i*2:], float32ToFloat16(float32(v*inv)))
		}
	case ddpF32:
		for i, v := range a.sum {
			binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(float32(v*inv)))
		}
	case ddpF64:
		for i, v := range a.sum {
			binary.LittleEndian.PutUint64(out[i*8:], math.Float64bits(v*inv))
		}
	case ddpI8:
		peak := 0.0
		for _, v := range a.sum {
			if m := math.Abs(v * inv); m > peak {
				peak = m
			}
		}
		if peak == 0 {
			return out, 1, nil // all zeros; any positive scale decodes correctly
		}
		scale := peak / 127
		for i, v := range a.sum {
			q := math.Round(v * inv / scale)
			// Clamp rather than wrap: a wrapped gradient flips sign, and a
			// gradient that flips sign is worse than one that saturates.
			if q > 127 {
				q = 127
			} else if q < -127 {
				q = -127
			}
			out[i] = byte(int8(q))
		}
		return out, scale, nil
	default:
		return nil, 0, fmt.Errorf("unsupported dtype %q", a.dtype)
	}
	return out, 0, nil
}

// float16ToFloat32 decodes IEEE 754 binary16. Written out rather than pulled
// in: it is twenty lines, and the alternative is a dependency in the hot path
// of every training step.
func float16ToFloat32(h uint16) float32 {
	sign := uint32(h&0x8000) << 16
	exp := (h >> 10) & 0x1f
	mant := uint32(h & 0x03ff)

	switch exp {
	case 0:
		if mant == 0 {
			return math.Float32frombits(sign) // signed zero
		}
		// Subnormal: normalise it into float32's range.
		e := uint32(127 - 15 + 1)
		for mant&0x0400 == 0 {
			mant <<= 1
			e--
		}
		mant &= 0x03ff
		return math.Float32frombits(sign | e<<23 | mant<<13)
	case 0x1f:
		// Inf or NaN; the mantissa distinguishes them and shifts intact.
		return math.Float32frombits(sign | 0xff<<23 | mant<<13)
	default:
		return math.Float32frombits(sign | (uint32(exp)-15+127)<<23 | mant<<13)
	}
}

// float32ToFloat16 encodes IEEE 754 binary16, rounding to nearest even.
// Values beyond binary16's range saturate to infinity rather than wrapping,
// which is what every framework's fp16 path does.
func float32ToFloat16(f float32) uint16 {
	b := math.Float32bits(f)
	sign := uint16(b>>16) & 0x8000
	exp := int32((b>>23)&0xff) - 127
	mant := b & 0x007fffff

	switch {
	case (b>>23)&0xff == 0xff:
		if mant != 0 {
			// NaN: keep it a NaN, and keep at least one mantissa bit so it
			// does not decay into infinity.
			m := uint16(mant >> 13)
			if m == 0 {
				m = 1
			}
			return sign | 0x7c00 | m
		}
		return sign | 0x7c00
	case exp > 15:
		return sign | 0x7c00 // saturate rather than wrap
	case exp < -25:
		// Below half the smallest subnormal, so it rounds to zero. The
		// boundary is -25 and not -24: a value in [2^-25, 2^-24) still rounds
		// up to the smallest subnormal, and cutting at -24 silently flushed
		// those to zero. Caught by checking this codec against numpy rather
		// than against its author's expectations.
		return sign
	case exp < -14:
		// Subnormal in binary16: the result is value/2^-24, rounded.
		mant |= 0x00800000 // restore the implicit leading bit
		shift := uint32(-exp - 1)
		q := mant >> shift
		rem := mant & ((uint32(1) << shift) - 1)
		half := uint32(1) << (shift - 1)
		// Round half to even, as IEEE 754 requires and as every framework's
		// fp16 path does. Truncating instead biases every value towards zero,
		// which over thousands of steps is a drift rather than noise.
		if rem > half || (rem == half && q&1 == 1) {
			q++
		}
		// A carry out of the mantissa is correct: the value becomes the
		// smallest normal, and the exponent field picks it up for free.
		return sign | uint16(q)
	default:
		// Round to nearest even at bit 13.
		lsb := (mant >> 13) & 1
		rounded := mant + 0x0fff + lsb
		if rounded&0x00800000 != 0 { // carried into the exponent
			exp++
			rounded = 0
			if exp > 15 {
				return sign | 0x7c00
			}
		}
		return sign | uint16(exp+15)<<10 | uint16(rounded>>13)
	}
}
