package daemonapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

func BenchmarkBuildFrames(b *testing.B) {
	items := make([]json.RawMessage, 256)
	for i := range items {
		items[i] = make([]byte, 4096)
	}
	hdr := []byte(`{"items_frames":256,"globals":true}`)
	globals := make([]byte, 1<<16)
	b.ReportAllocs()
	for b.Loop() {
		_ = buildFrames(hdr, globals, items)
	}
}

func benchResults() []any {
	res := make([]any, 256)
	for i := range res {
		res[i] = map[string]string{"pickle": string(make([]byte, 4096))}
	}
	return res
}

func BenchmarkResultFrames(b *testing.B) {
	res := benchResults()
	hdr := []byte("{\"results_frames\": 256}\n")
	b.ReportAllocs()
	for b.Loop() {
		_ = buildResultFrames(hdr, res)
	}
}

func BenchmarkReadBody(b *testing.B) {
	body := make([]byte, 8<<20)
	b.ReportAllocs()
	for b.Loop() {
		r, _ := http.NewRequest("POST", "http://x/v1/pool/map", bytes.NewReader(body))
		got, err := readAllSized(r.Body, r.ContentLength)
		if err != nil || len(got) != len(body) {
			b.Fatalf("read %d bytes, err %v", len(got), err)
		}
	}
}

func TestResultFramesLayout(t *testing.T) {
	res := []any{map[string]string{"pickle": "abc"}, map[string]string{"pickle": ""}, "x", map[string]string{"pickle": "hello"}}
	hdr := "{\"results_frames\": 4}\n"
	want := hdr + "\x00\x00\x00\x03abc" + "\x00\x00\x00\x00" + "\x00\x00\x00\x00" + "\x00\x00\x00\x05hello"
	if got := buildResultFrames([]byte(hdr), res); string(got) != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestBuildFramesLayout(t *testing.T) {
	got := buildFrames([]byte(`{"h":1}`), []byte("gg"), []json.RawMessage{[]byte("a"), []byte("")})
	want := "{\"h\":1}\n" + "\x00\x00\x00\x02gg" + "\x00\x00\x00\x01a" + "\x00\x00\x00\x00"
	if string(got) != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestReadAllSizedWrongLength(t *testing.T) {
	body := []byte("0123456789")
	for _, n := range []int64{-1, 0, 3, 10, 64} {
		got, err := readAllSized(bytes.NewReader(body), n)
		if err != nil || string(got) != string(body) {
			t.Fatalf("n=%d: got %q, err %v", n, got, err)
		}
	}
}
