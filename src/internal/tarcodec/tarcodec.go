// Package tarcodec compresses the archives that cross between machines, and
// reads whichever encoding arrives.
//
// Three encodings are readable and one is written. That asymmetry is the
// point: a cluster is upgraded one machine at a time, so a new submitter will
// meet an old daemon and an old submitter a new one, in both directions, for
// as long as the rollout takes. Sniffing the first bytes costs nothing and
// removes the flag day.
package tarcodec

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

// Encoding names what an archive is wrapped in.
type Encoding string

const (
	Plain Encoding = "plain"
	Gzip  Encoding = "gzip"
	Zstd  Encoding = "zstd"
)

// magic bytes, as each format writes them.
var (
	gzipMagic = []byte{0x1f, 0x8b}
	zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}
)

// Sniff reports what an archive is wrapped in, without consuming it.
func Sniff(br *bufio.Reader) Encoding {
	if m, err := br.Peek(4); err == nil && string(m) == string(zstdMagic) {
		return Zstd
	}
	if m, err := br.Peek(2); err == nil && string(m) == string(gzipMagic) {
		return Gzip
	}
	return Plain
}

// Reader unwraps an archive whatever it arrived as. The returned close
// function is always non-nil.
func Reader(r io.Reader) (io.Reader, func(), error) {
	br := bufio.NewReader(r)
	switch Sniff(br) {
	case Zstd:
		z, err := zstd.NewReader(br)
		if err != nil {
			return nil, func() {}, err
		}
		return z, z.Close, nil
	case Gzip:
		g, err := gzip.NewReader(br)
		if err != nil {
			return nil, func() {}, err
		}
		return g, func() { g.Close() }, nil
	default:
		return br, func() {}, nil
	}
}

// Writer wraps w so what is written to it comes out compressed.
//
// Fastest level, not best. These archives are written once and read once, on
// the far side of a link that is usually the bottleneck; spending CPU to save
// a few more percent is the wrong trade, and this runs on the machine the
// user is waiting at.
func Writer(w io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
}

// CompressFile rewrites a file compressed, in place.
//
// Small files are left alone: under the floor the framing and the second pass
// over the disk cost more than the bytes saved on any link worth having.
func CompressFile(path string, floor int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() < floor {
		return nil
	}

	src, err := os.Open(path)
	if err != nil {
		return err
	}
	defer src.Close()

	tmp := path + ".zst"
	dst, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc, err := Writer(dst)
	if err != nil {
		dst.Close()
		os.Remove(tmp)
		return err
	}
	if _, err := io.Copy(enc, src); err != nil {
		enc.Close()
		dst.Close()
		os.Remove(tmp)
		return err
	}
	if err := enc.Close(); err != nil {
		dst.Close()
		os.Remove(tmp)
		return err
	}
	if err := dst.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// CompressFloor is the size below which an archive travels as it is. A
// workspace of a few source files is already smaller than the round trip that
// carries it.
const CompressFloor = 1 << 20
