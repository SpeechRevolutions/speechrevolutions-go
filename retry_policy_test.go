package stt

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Retry policy, and the duplicate-job hazard it exists to prevent.
//
// The rule under test: a request that CREATES a job is retried only when the
// request provably never reached the server. Every other request retries
// freely.
//
// A regression here is expensive and silent — the customer gets two
// transcripts and two charges for one file — so these assert attempt COUNTS,
// not just the final outcome.

const okBody = `{"job_id":"j1","upload_url":"u","download_url":"d","content_type":"audio/mpeg","expires_in":900}`

// newClient points a Client at a test server and makes retries instant.
func newClient(t *testing.T, h http.HandlerFunc) (*Client, *httptest.Server, *int32) {
	t.Helper()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	c := &Client{
		APIKey:       "k",
		BaseURL:      srv.URL,
		Timeout:      10 * time.Second,
		HTTP:         srv.Client(),
		Multipart:    true,
		MaxRetries:   3,
		RetryBackoff: time.Millisecond,
	}
	return c, srv, &calls
}

func status(code int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}
}

// ---------------------------------------------------------------------------
// createsJob — the single source of truth for the policy
// ---------------------------------------------------------------------------

func TestCreatesJobClassification(t *testing.T) {
	cases := map[string]bool{
		"/api/v1/upload":                    true,
		"/api/v1/upload/":                   true,
		"/api/v1/upload?x=1":                true,
		"/api/v1/upload/multipart/create":   true,
		"/api/v1/upload/complete":           false,
		"/api/v1/upload/progress":           false,
		"/api/v1/upload/multipart/complete": false,
		"/api/v1/upload/multipart/abort":    false,
		"/api/v1/jobs/cancel":               false,
		"/api/v1/jobs/check-failed":         false,
		"/api/v1/jobs":                      false,
		"/api/v1/jobs/abc123":               false,
	}
	for path, want := range cases {
		if got := createsJob(path); got != want {
			t.Errorf("createsJob(%q) = %v, want %v", path, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// neverReachedServer — what counts as "provably not delivered"
// ---------------------------------------------------------------------------

func TestNeverReachedServer(t *testing.T) {
	if !neverReachedServer(&net.DNSError{Err: "no such host", Name: "x"}) {
		t.Error("a DNS failure means no connection was established")
	}
	if !neverReachedServer(&net.OpError{Op: "dial", Err: errors.New("connection refused")}) {
		t.Error("a dial-stage OpError means no connection was established")
	}
	// A read-stage failure is ambiguous: the request may already have been
	// processed and only the response lost.
	if neverReachedServer(&net.OpError{Op: "read", Err: errors.New("reset by peer")}) {
		t.Error("a read-stage OpError is ambiguous and must not be retried for a create")
	}
	if neverReachedServer(errors.New("something else entirely")) {
		t.Error("an unrecognised error must be treated as ambiguous")
	}
}

// ---------------------------------------------------------------------------
// Create calls: must NOT retry on an ambiguous failure
// ---------------------------------------------------------------------------

func TestCreateDoesNotRetryOn5xx(t *testing.T) {
	for _, code := range []int{500, 502, 503, 504} {
		c, _, calls := newClient(t, status(code, `{"detail":"boom"}`))
		_, err := c.CreateUploadJob(context.Background(), 1024, TranscribeOptions{})
		if err == nil {
			t.Fatalf("status %d: expected an error", code)
		}
		if n := atomic.LoadInt32(calls); n != 1 {
			t.Errorf("status %d: made %d attempts, want 1 — a retry would create a duplicate job", code, n)
		}
	}
}

func TestMultipartCreateDoesNotRetryOn5xx(t *testing.T) {
	c, _, calls := newClient(t, status(503, `{"detail":"unavailable"}`))
	err := c.apiRequest(context.Background(), "POST", "/api/v1/upload/multipart/create", nil, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if n := atomic.LoadInt32(calls); n != 1 {
		t.Errorf("made %d attempts, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Create calls: SHOULD retry when the request provably never landed
// ---------------------------------------------------------------------------

func TestCreateRetriesOn429(t *testing.T) {
	var n int32
	c, _, calls := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"detail":"slow down"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(okBody))
	})
	job, err := c.CreateUploadJob(context.Background(), 1024, TranscribeOptions{})
	if err != nil {
		t.Fatalf("a 429 is refused before any work and must be retried: %v", err)
	}
	if job.JobID != "j1" {
		t.Errorf("job id = %q, want j1", job.JobID)
	}
	if got := atomic.LoadInt32(calls); got != 2 {
		t.Errorf("made %d attempts, want 2", got)
	}
}

func TestCreateRetriesWhenTheServerIsUnreachable(t *testing.T) {
	// A closed port produces a dial-stage error: nothing was ever delivered.
	c, srv, _ := newClient(t, status(200, okBody))
	srv.Close()
	_, err := c.CreateUploadJob(context.Background(), 1024, TranscribeOptions{})
	if err == nil {
		t.Fatal("expected an error once retries are exhausted")
	}
}

// ---------------------------------------------------------------------------
// Non-create calls keep the permissive behaviour
// ---------------------------------------------------------------------------

func TestSafePathRetriesOnTransientStatuses(t *testing.T) {
	for _, code := range []int{429, 500, 502, 503, 504} {
		var n int32
		c, _, calls := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
			if atomic.AddInt32(&n, 1) == 1 {
				w.WriteHeader(code)
				_, _ = w.Write([]byte(`{}`))
				return
			}
			_, _ = w.Write([]byte(`{}`))
		})
		if err := c.CancelJob(context.Background(), "j1"); err != nil {
			t.Fatalf("status %d: %v", code, err)
		}
		if got := atomic.LoadInt32(calls); got != 2 {
			t.Errorf("status %d: made %d attempts, want 2", code, got)
		}
	}
}

func TestCompleteUploadIsRetryable(t *testing.T) {
	// complete acts on a job id the caller already holds, so replay is harmless.
	var n int32
	c, _, calls := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	})
	if err := c.CompleteUpload(context.Background(), "j1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := atomic.LoadInt32(calls); got != 2 {
		t.Errorf("made %d attempts, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// The scenario this whole policy exists for
// ---------------------------------------------------------------------------

func TestLostResponseCreatesExactlyOneJob(t *testing.T) {
	// The server creates the job, then the response is lost to a gateway 502.
	// The SDK must surface the error rather than silently creating a second.
	c, _, calls := newClient(t, status(502, `{"detail":"bad gateway"}`))
	_, err := c.CreateUploadJob(context.Background(), 1024, TranscribeOptions{})
	if err == nil {
		t.Fatal("expected the 502 to surface")
	}
	if n := atomic.LoadInt32(calls); n != 1 {
		t.Fatalf("made %d attempts, want 1 — a retry here would have created a duplicate job", n)
	}
}
