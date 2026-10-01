package stt

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
)

// TranscribeOptions.Language must reach every job-creating request as
// "language", and be absent entirely when unset so an older server sees a
// byte-identical request.

// captureBodies records the JSON body of every request to path.
func captureBodies(t *testing.T, path string, h http.HandlerFunc) (*Client, func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var bodies []map[string]any
	c, _, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == path {
			raw, _ := io.ReadAll(r.Body)
			var m map[string]any
			if err := json.Unmarshal(raw, &m); err != nil {
				t.Errorf("request body to %s is not JSON: %v", path, err)
			}
			mu.Lock()
			bodies = append(bodies, m)
			mu.Unlock()
		}
		h(w, r)
	})
	return c, func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]any(nil), bodies...)
	}
}

func checkLanguage(t *testing.T, bodies []map[string]any, want string) {
	t.Helper()
	if len(bodies) == 0 {
		t.Fatal("no request captured")
	}
	got, present := bodies[0]["language"]
	if want == "" {
		if present {
			t.Errorf("language present when unset: %v", got)
		}
		return
	}
	if got != want {
		t.Errorf("language = %v (present=%v), want %q", got, present, want)
	}
}

func TestUploadBodyLanguage(t *testing.T) {
	raw, err := json.Marshal(uploadBody(10, TranscribeOptions{Language: "ru"}))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if m["language"] != "ru" {
		t.Errorf("language = %v, want \"ru\"", m["language"])
	}

	raw, err = json.Marshal(uploadBody(10, TranscribeOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	m = nil
	_ = json.Unmarshal(raw, &m)
	if _, ok := m["language"]; ok {
		t.Errorf("language present when unset: %s", raw)
	}
}

func TestLanguageSingleShotUpload(t *testing.T) {
	for _, lang := range []string{"en", ""} {
		c, bodies := captureBodies(t, "/api/v1/upload", status(200, okBody))
		if _, err := c.CreateUploadJob(context.Background(), 1024, TranscribeOptions{Language: lang}); err != nil {
			t.Fatalf("CreateUploadJob(%q): %v", lang, err)
		}
		checkLanguage(t, bodies(), lang)
	}
}

func TestLanguageSubmitURL(t *testing.T) {
	for _, lang := range []string{"ru", ""} {
		c, bodies := captureBodies(t, "/api/v1/upload", status(200, okBody))
		if _, err := c.SubmitURL(context.Background(), "https://example.com/a.mp3", TranscribeOptions{Language: lang}); err != nil {
			t.Fatalf("SubmitURL(%q): %v", lang, err)
		}
		checkLanguage(t, bodies(), lang)
	}
}

func TestLanguageMultipartCreate(t *testing.T) {
	for _, lang := range []string{"de", ""} {
		// 404 = multipart disabled; the body was still sent, which is all we check.
		c, bodies := captureBodies(t, "/api/v1/upload/multipart/create", status(404, `{"detail":"not found"}`))
		_, _, _ = c.uploadMultipart(context.Background(), []byte("audio"), TranscribeOptions{Language: lang}, nil)
		checkLanguage(t, bodies(), lang)
	}
}
