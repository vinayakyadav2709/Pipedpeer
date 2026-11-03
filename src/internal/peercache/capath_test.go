package peercache

import "testing"

func TestCAStorePath(t *testing.T) {
	// Expected paths come from `nix-store --print-fixed-path` on nix 2.x.
	abc256 := "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s" // sha256("abc")
	abc1 := "kpcd173cq987hw957sx6m0868wv3x6d9"                       // sha1("abc")
	cases := []struct {
		name, ca, path string
		refs           []string
		want           string // "" = error
	}{
		{"recursive sha256", "fixed:r:sha256:" + abc256, "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo-1.0", nil, "/nix/store/9mvyn7m5bp9mqnzzkxr63c48gxr3r3z6-foo-1.0"},
		{"flat sha256", "fixed:sha256:" + abc256, "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo-1.0", nil, "/nix/store/mv8dnc3mq5ws05l4frs8yk24gmr74rv7-foo-1.0"},
		{"recursive sha1", "fixed:r:sha1:" + abc1, "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo-1.0", nil, "/nix/store/2l6jwx8inv08jqjcmwav936kz8kk482g-foo-1.0"},
		{"text is unsupported", "text:sha256:" + abc256, "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo-1.0", nil, ""},
		{"foreign dir", "fixed:sha256:" + abc256, "/evil/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo-1.0", nil, ""},
		{"bad hash", "fixed:sha256:xyz", "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo-1.0", nil, ""},
	}
	for _, c := range cases {
		got, err := CAStorePath(c.ca, c.path, c.refs)
		if (err == nil) != (c.want != "") || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", c.name, got, err, c.want)
		}
	}
}
