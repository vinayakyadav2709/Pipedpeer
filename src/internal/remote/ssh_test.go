package remote

import "testing"

func TestParseRemoteWithUserAndPort(t *testing.T) {
	cfg, err := Parse("alice@example.com:2222")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if cfg.User != "alice" || cfg.Host != "example.com" || cfg.Port != 2222 {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestParseRemoteHostOnly(t *testing.T) {
	cfg, err := Parse("example.com")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if cfg.User != "root" || cfg.Host != "example.com" || cfg.Port != 22 {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestParseRemoteHostWithPortNoUser(t *testing.T) {
	cfg, err := Parse("example.com:2022")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if cfg.User != "root" || cfg.Host != "example.com" || cfg.Port != 2022 {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}
