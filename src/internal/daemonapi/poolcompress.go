package daemonapi

import (
	"io"
	"math"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

// compressChoice is the Dask-style sample test (R9 a1): five 16 KiB samples
// at the fastest level give the ratio and the compressor speed C. Compression
// is kept only when the samples shrink to <= 0.8 and C (scaled for the level)
// is at least 1.5x the link speed B; the level follows B: under 5 MB/s
// better, under 60 MB/s default, else fastest. An unknown link gets fastest.
// trialed is the bytes compressed to decide (the cost on incompressible input).
func compressChoice(body []byte, bps float64, known bool) (lvl zstd.EncoderLevel, ok bool, trialed int) {
	if len(body) < compressMinBody {
		return 0, false, 0
	}
	enc := encoderFor(zstd.SpeedFastest)
	step := (len(body) - trialSampleSize) / (trialSamples - 1)
	var packed int
	dst := make([]byte, 0, trialSampleSize+64)
	start := time.Now()
	for i := 0; i < trialSamples; i++ {
		s := body[i*step : i*step+trialSampleSize]
		dst = enc.EncodeAll(s, dst[:0])
		packed += len(dst)
		trialed += len(s)
	}
	took := time.Since(start).Seconds()
	if float64(packed) > 0.8*float64(trialed) {
		return 0, false, trialed
	}
	if !known {
		return zstd.SpeedFastest, true, trialed
	}
	c := float64(trialed) / max(took, 1e-9)
	// Relative speed of each level against fastest (klauspost README:
	// 318 / 134 / 67 MB/s), so the measured C carries over.
	for _, l := range []struct {
		lvl   zstd.EncoderLevel
		below float64
		speed float64
	}{
		{zstd.SpeedBetterCompression, 5e6, 0.21},
		{zstd.SpeedDefault, 60e6, 0.42},
		{zstd.SpeedFastest, math.Inf(1), 1},
	} {
		if bps < l.below && c*l.speed >= 1.5*bps {
			return l.lvl, true, trialed
		}
	}
	return 0, false, trialed
}

var (
	encMu    sync.Mutex
	encoders = map[zstd.EncoderLevel]*zstd.Encoder{}
	decOnce  sync.Once
	decoder  *zstd.Decoder
)

// encoderFor returns a shared encoder; EncodeAll is safe for concurrent use.
func encoderFor(l zstd.EncoderLevel) *zstd.Encoder {
	encMu.Lock()
	defer encMu.Unlock()
	if e := encoders[l]; e != nil {
		return e
	}
	e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(l))
	if err != nil {
		panic("zstd encoder with static options: " + err.Error()) // only bad options fail
	}
	encoders[l] = e
	return e
}

// zstdDecoder is shared; DecodeAll is safe for concurrent use. The output is
// capped like an uncompressed body, so a small bomb cannot grow past it.
func zstdDecoder() *zstd.Decoder {
	decOnce.Do(func() {
		d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxPoolBody))
		if err != nil {
			panic("zstd decoder with static options: " + err.Error())
		}
		decoder = d
	})
	return decoder
}

// observeLink feeds one real transfer on the link to a peer into its EWMA:
// bytes that crossed and the time they took. U18 calls it through timedRead
// for replies, and for requests with (time to response headers - the peer's
// reported compute). Small transfers measure latency and are ignored.
func observeLink(peerKey string, n int64, d time.Duration) {
	if n < linkMinSample || d <= 0 {
		return
	}
	bps := float64(n) / d.Seconds()
	links.Lock()
	defer links.Unlock()
	l := linkFor(peerKey)
	if l.bps == 0 || time.Since(l.at) > linkFresh {
		l.bps = bps
	} else {
		l.bps = linkEWMA*bps + (1-linkEWMA)*l.bps
	}
	l.at = time.Now()
}

// timedRead reads a peer's reply body and records how fast it arrived.
func timedRead(peerKey string, r io.Reader) ([]byte, error) {
	start := time.Now()
	b, err := io.ReadAll(r)
	if err == nil {
		observeLink(peerKey, int64(len(b)), time.Since(start))
	}
	return b, err
}

// linkSpeed is the measured bytes/s on the link to a peer (EWMA of real
// transfers); ok=false when there is no recent sample.
func linkSpeed(peerKey string) (bytesPerSec float64, ok bool) {
	links.Lock()
	defer links.Unlock()
	l := links.m[peerKey]
	if l == nil || l.bps == 0 || time.Since(l.at) > linkFresh {
		return 0, false
	}
	return l.bps, true
}

// clusterLinkSpeed is the sum of every fresh per-peer speed: the aggregate
// the shim's cost models expect (numpy_ops divides it per peer). It is the
// additive link_bytes_per_sec field of /v1/pool/stats; ok=false omits it and
// the shim falls back to a small probe.
func clusterLinkSpeed() (bytesPerSec float64, ok bool) {
	links.Lock()
	defer links.Unlock()
	for _, l := range links.m {
		if l.bps > 0 && time.Since(l.at) <= linkFresh {
			bytesPerSec += l.bps
			ok = true
		}
	}
	return bytesPerSec, ok
}
