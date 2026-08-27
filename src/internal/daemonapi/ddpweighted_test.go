package daemonapi

import (
	"encoding/binary"
	"math"
	"testing"
)

// f32Payload encodes values the way a rank sends them.
func f32Payload(vals []float64) []byte {
	out := make([]byte, len(vals)*4)
	for i, v := range vals {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(float32(v)))
	}
	return out
}

func f32Decode(b []byte) []float64 {
	out := make([]float64, len(b)/4)
	for i := range out {
		out[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:])))
	}
	return out
}

// TestUnequalBatchesAverageBySampleCount.
//
// This is the correctness argument for proportional batches, not a refinement
// of it. A rank training on twice the data computed its mean gradient over
// twice the samples; averaging the two per-rank means as equals gives the
// smaller shard twice the influence per sample it earned, and the ring stops
// computing the gradient the single-process run would have.
func TestUnequalBatchesAverageBySampleCount(t *testing.T) {
	// Rank 0 saw 3000 samples and got 1.0; rank 1 saw 1000 and got 5.0.
	// The single-process gradient over all 4000 is
	//   (3000*1.0 + 1000*5.0) / 4000 = 2.0
	// The plain average would say 3.0, which is the gradient of no dataset.
	a := newDDPAccumulator(ddpF32, 2)
	if err := a.addSegmented(f32Payload([]float64{1.0, 1.0}), []float64{0}, 3000); err != nil {
		t.Fatal(err)
	}
	if err := a.addSegmented(f32Payload([]float64{5.0, 5.0}), []float64{0}, 1000); err != nil {
		t.Fatal(err)
	}
	out, _, err := a.mean()
	if err != nil {
		t.Fatal(err)
	}
	got := f32Decode(out)
	for i, v := range got {
		if math.Abs(v-2.0) > 1e-6 {
			t.Errorf("element %d averaged to %v, want 2.0 (the gradient over all "+
				"4000 samples). 3.0 would mean the shards were weighed as equals",
				i, v)
		}
	}
}

// TestEqualWeightsReproduceThePlainAverage, so a ring that has not been told
// about sample counts behaves exactly as it did before.
func TestEqualWeightsReproduceThePlainAverage(t *testing.T) {
	weighted := newDDPAccumulator(ddpF32, 2)
	plain := newDDPAccumulator(ddpF32, 2)
	for _, v := range []float64{1.0, 2.0, 6.0} {
		p := f32Payload([]float64{v, -v})
		if err := weighted.addSegmented(p, []float64{0}, 500); err != nil {
			t.Fatal(err)
		}
		if err := plain.add(p, 0); err != nil {
			t.Fatal(err)
		}
	}
	w, _, err := weighted.mean()
	if err != nil {
		t.Fatal(err)
	}
	pl, _, err := plain.mean()
	if err != nil {
		t.Fatal(err)
	}
	for i := range f32Decode(w) {
		if math.Abs(f32Decode(w)[i]-f32Decode(pl)[i]) > 1e-9 {
			t.Errorf("equal weights diverged from the plain average at %d: %v vs %v",
				i, f32Decode(w)[i], f32Decode(pl)[i])
		}
	}
}

// TestASilentRankWeighsOne. A rank running an older shim sends no sample
// count; reading that as zero would drop its gradient entirely and, worse,
// leave the divisor short so every other rank's contribution came out
// inflated.
func TestASilentRankWeighsOne(t *testing.T) {
	a := newDDPAccumulator(ddpF32, 1)
	if err := a.addSegmented(f32Payload([]float64{4.0}), []float64{0}, 0); err != nil {
		t.Fatal(err)
	}
	if err := a.addSegmented(f32Payload([]float64{2.0}), []float64{0}, 0); err != nil {
		t.Fatal(err)
	}
	out, _, err := a.mean()
	if err != nil {
		t.Fatal(err)
	}
	if got := f32Decode(out)[0]; math.Abs(got-3.0) > 1e-6 {
		t.Errorf("two silent ranks averaged to %v, want 3.0", got)
	}
}

// TestWeightedInt8KeepsPerTensorScales. The int8 path folds each tensor with
// its own scale; weighting must multiply the dequantised value, not the
// quantised one, or the weight interacts with the step size.
func TestWeightedInt8KeepsPerTensorScales(t *testing.T) {
	a := newDDPAccumulator(ddpI8, 4)
	if err := a.withSegments([]int{2, 2}); err != nil {
		t.Fatal(err)
	}
	// Two tensors with scales an order of magnitude apart, as a real model's
	// output and hidden layers are.
	big, small := 0.1, 0.001
	// rank 0: 3000 samples, quantised values 10 and 100
	if err := a.addSegmented([]byte{10, 10, 100, 100}, []float64{big, small}, 3000); err != nil {
		t.Fatal(err)
	}
	// rank 1: 1000 samples, same quantised values
	if err := a.addSegmented([]byte{10, 10, 100, 100}, []float64{big, small}, 1000); err != nil {
		t.Fatal(err)
	}
	_, scales, err := a.meanSegmented()
	if err != nil {
		t.Fatal(err)
	}
	if len(scales) != 2 {
		t.Fatalf("got %d scales, want one per tensor", len(scales))
	}
	// Both ranks sent identical values, so the weighted mean equals them:
	// tensor 0 is 10*0.1 = 1.0, tensor 1 is 100*0.001 = 0.1. A scale derived
	// from a peak of 1.0 and one from 0.1 must differ by that factor.
	if scales[0] <= scales[1]*2 {
		t.Errorf("scales %v do not reflect the tensors' different magnitudes; "+
			"one scale for both is what cost 0.087 -> 0.127 on the demo", scales)
	}
}

// TestNoRanksIsAnError rather than a divide by zero producing NaN gradients
// that train a model into nonsense without failing.
func TestNoRanksIsAnError(t *testing.T) {
	a := newDDPAccumulator(ddpF32, 1)
	if _, _, err := a.mean(); err == nil {
		t.Error("an empty accumulator produced a mean")
	}
}
