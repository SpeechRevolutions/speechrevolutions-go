package stt

import (
	"regexp"
	"strings"
	"testing"
)

// The edge answers a request with no User-Agent with a bare 403, so this header
// is load-bearing rather than decorative. It used to be a literal beside a
// version that lives in a git tag; the Python SDK shipped exactly that shape and
// served 0.2.2 from PyPI while announcing 0.2.0 on the wire.

func TestUserAgentPrefix(t *testing.T) {
	if !strings.HasPrefix(userAgent, "speechrevolutions-go/") {
		t.Fatalf("userAgent = %q, want speechrevolutions-go/ prefix", userAgent)
	}
}

func TestUserAgentCarriesAVersion(t *testing.T) {
	version := strings.TrimPrefix(userAgent, "speechrevolutions-go/")
	if version == "" {
		t.Fatal("userAgent carries no version")
	}
	// Either a real semver, or the honest dev sentinel -- never a bare literal
	// someone forgot to bump.
	ok := regexp.MustCompile(`^\d+\.\d+\.\d+`).MatchString(version)
	if !ok {
		t.Fatalf("version %q is not version-shaped", version)
	}
	if strings.HasPrefix(version, "v") {
		t.Fatalf("version %q should not keep the leading v", version)
	}
}

func TestNormalizeVersion(t *testing.T) {
	for in, want := range map[string]string{
		"v0.2.1":  "0.2.1",
		"0.2.1":   "0.2.1",
		"":        "0.0.0-dev",
		"(devel)": "0.0.0-dev",
	} {
		if got := normalizeVersion(in); got != want {
			t.Errorf("normalizeVersion(%q) = %q, want %q", in, got, want)
		}
	}
}
