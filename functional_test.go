package stt

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The published surface, end to end against a real server.
//
// The retry tests use httptest handlers; these drive the API mock from the
// python-sdk checkout — the same reference implementation the Python suites,
// the Node suite and the cookbook tests use, so none of them can drift from
// each other or from the contract.
//
// Shapes here match production, verified live on 2026-09-18: job ids are
// UUIDs, a job summary is {job_id, created_at} with no status, the list cursor
// is a created_at timestamp, and a completion webhook carries a presigned
// download_url alongside duration_seconds and rtf.
//
// Skipped when the mock is not found. Override with SR_MOCK_API.

func mockPath() string {
	if p := os.Getenv("SR_MOCK_API"); p != "" {
		return p
	}
	wd, _ := os.Getwd()
	return filepath.Join(wd, "..", "python-sdk", "tests", "mock_api.py")
}

func pythonBin() string {
	if p := os.Getenv("SR_PYTHON"); p != "" {
		return p
	}
	return "python3"
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("no free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type mockServer struct {
	base string
	cmd  *exec.Cmd
}

// startMock boots mock_api.py and waits for it to answer.
func startMock(t *testing.T, extra ...string) *mockServer {
	t.Helper()
	path := mockPath()
	if _, err := os.Stat(path); err != nil {
		t.Skipf("API mock not found at %s (set SR_MOCK_API)", path)
	}

	port := freePort(t)
	args := append([]string{
		path, "--port", fmt.Sprint(port), "--api-key", "test-key",
		"--progress-steps", "2",
	}, extra...)
	cmd := exec.Command(pythonBin(), args...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("cannot start mock: %v", err)
	}

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("GET", base+"/api/v1/jobs", nil)
		req.Header.Set("X-API-Key", "test-key")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
			ms := &mockServer{base: base, cmd: cmd}
			t.Cleanup(ms.stop)
			return ms
		}
		time.Sleep(150 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	t.Fatal("mock never came up")
	return nil
}

func (m *mockServer) stop() {
	if m.cmd != nil && m.cmd.Process != nil {
		_ = m.cmd.Process.Kill()
		_, _ = m.cmd.Process.Wait()
	}
}

func liveClient(t *testing.T, base string) *Client {
	t.Helper()
	c, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.BaseURL = base
	c.RetryBackoff = time.Millisecond
	return c
}

var uuidRe = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func audioBytes() []byte { return []byte(strings.Repeat("x", 4096)) }

// ---------------------------------------------------------------------------
// Base URL resolution
// ---------------------------------------------------------------------------

func TestResolveBaseURL(t *testing.T) {
	t.Setenv("SPEECHREVOLUTIONS_BASE_URL", "")
	if got := resolveBaseURL(""); got != defaultBaseURL {
		t.Errorf("default = %q, want %q", got, defaultBaseURL)
	}
	if got := resolveBaseURL("https://explicit.example/"); got != "https://explicit.example" {
		t.Errorf("explicit = %q (trailing slash should be stripped)", got)
	}

	t.Setenv("SPEECHREVOLUTIONS_BASE_URL", "https://staging.example/")
	if got := resolveBaseURL(""); got != "https://staging.example" {
		t.Errorf("env = %q", got)
	}
	if got := resolveBaseURL("https://explicit.example"); got != "https://explicit.example" {
		t.Errorf("explicit must win over env, got %q", got)
	}

	// STT_BASE_URL predates the rebrand; honouring it would let a stale
	// variable silently point the client at the wrong host.
	t.Setenv("SPEECHREVOLUTIONS_BASE_URL", "")
	t.Setenv("STT_BASE_URL", "https://stale.example")
	if got := resolveBaseURL(""); got != defaultBaseURL {
		t.Errorf("pre-rebrand STT_BASE_URL must be ignored, got %q", got)
	}
}

func TestNewClientPicksUpEnvBaseURL(t *testing.T) {
	t.Setenv("SPEECHREVOLUTIONS_BASE_URL", "https://staging.example")
	c, err := NewClient("k")
	if err != nil {
		t.Fatal(err)
	}
	if c.BaseURL != "https://staging.example" {
		t.Errorf("BaseURL = %q", c.BaseURL)
	}
}

// ---------------------------------------------------------------------------
// Uploads
// ---------------------------------------------------------------------------

func TestTranscribeBytes(t *testing.T) {
	m := startMock(t)
	got, err := liveClient(t, m.base).TranscribeBytes(
		context.Background(), audioBytes(), TranscribeOptions{}, nil)
	if err != nil {
		t.Fatalf("TranscribeBytes: %v", err)
	}
	if want := "Good morning everyone. Thanks for joining."; got.Text() != want {
		t.Errorf("Text = %q, want %q", got.Text(), want)
	}
	if len(got.Words) != 6 {
		t.Errorf("Words = %d, want 6", len(got.Words))
	}
	if len(got.Utterances) != 2 {
		t.Errorf("Utterances = %d, want 2", len(got.Utterances))
	}
	if got.Utterances[0].Speaker != "A" || got.Utterances[1].Speaker != "B" {
		t.Errorf("speakers = %v", []string{got.Utterances[0].Speaker, got.Utterances[1].Speaker})
	}
}

func TestTranscribeURLIsFetchedServerSide(t *testing.T) {
	m := startMock(t)
	got, err := liveClient(t, m.base).TranscribeURL(
		context.Background(), "https://example.com/a.mp3", TranscribeOptions{}, nil)
	if err != nil {
		t.Fatalf("TranscribeURL: %v", err)
	}
	if got.Text() == "" {
		t.Error("empty transcript")
	}
}

func TestSRTOutput(t *testing.T) {
	m := startMock(t)
	got, err := liveClient(t, m.base).TranscribeBytes(
		context.Background(), audioBytes(), TranscribeOptions{OutputType: "srt"}, nil)
	if err != nil {
		t.Fatalf("srt: %v", err)
	}
	if !strings.Contains(got.Text(), "-->") {
		t.Errorf("not SRT: %.80q", got.Text())
	}
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

func TestProgressCallbackFires(t *testing.T) {
	m := startMock(t)
	var seen []ProgressEvent
	_, err := liveClient(t, m.base).TranscribeBytes(
		context.Background(), audioBytes(), TranscribeOptions{},
		func(e ProgressEvent) { seen = append(seen, e) })
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("progress events = %d, want 2", len(seen))
	}
	for i, e := range seen {
		if e.Completed == nil || e.Total == nil {
			t.Fatalf("event %d has nil counters: %+v", i, e)
		}
		if *e.Completed != i+1 || *e.Total != 2 {
			t.Errorf("event %d = %d/%d, want %d/2", i, *e.Completed, *e.Total, i+1)
		}
	}
}

// ---------------------------------------------------------------------------
// Jobs API
// ---------------------------------------------------------------------------

func TestSubmitReturnsAUUID(t *testing.T) {
	m := startMock(t)
	id, err := liveClient(t, m.base).SubmitURL(
		context.Background(), "https://example.com/a.mp3", TranscribeOptions{})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !uuidRe.MatchString(id) {
		t.Errorf("job id %q is not a UUID; production ids are UUIDs", id)
	}
}

func TestGetJobStatusAndTranscript(t *testing.T) {
	m := startMock(t)
	c := liveClient(t, m.base)
	id, err := c.SubmitURL(context.Background(), "https://example.com/a.mp3", TranscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.GetJobStatus(context.Background(), id)
	if err != nil {
		t.Fatalf("GetJobStatus: %v", err)
	}
	if st.Status != "completed" || st.DownloadURL == "" {
		t.Errorf("status = %+v", st)
	}
	tr, err := c.GetTranscript(context.Background(), id, "json")
	if err != nil {
		t.Fatalf("GetTranscript: %v", err)
	}
	if tr.Text() == "" {
		t.Error("empty transcript")
	}
}

func TestListJobsPaginatesWithoutDuplicates(t *testing.T) {
	m := startMock(t)
	c := liveClient(t, m.base)
	for i := 0; i < 5; i++ {
		if _, err := c.SubmitURL(context.Background(),
			fmt.Sprintf("https://example.com/%d.mp3", i), TranscribeOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]bool{}
	before := ""
	for i := 0; i < 10; i++ {
		page, err := c.ListJobs(context.Background(), 2, before)
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		for _, j := range page.Jobs {
			if seen[j.JobID] {
				t.Fatalf("pagination returned %s twice", j.JobID)
			}
			seen[j.JobID] = true
			// Production JobSummary is {job_id, created_at}; there is no status.
			if j.CreatedAt == "" {
				t.Errorf("job %s has no created_at", j.JobID)
			}
		}
		before = page.NextBefore
		if before == "" {
			break
		}
	}
	if len(seen) != 5 {
		t.Errorf("saw %d jobs, want 5", len(seen))
	}
}

func TestCheckFailedAndCancel(t *testing.T) {
	m := startMock(t)
	c := liveClient(t, m.base)
	id, err := c.SubmitURL(context.Background(), "https://example.com/a.mp3", TranscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	flags, err := c.CheckFailed(context.Background(), []string{id})
	if err != nil {
		t.Fatalf("CheckFailed: %v", err)
	}
	if len(flags) != 1 || flags[0] {
		t.Errorf("CheckFailed = %v, want [false]", flags)
	}

	job, err := c.CreateUploadJob(context.Background(), 1024, TranscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CancelJob(context.Background(), job.JobID); err != nil {
		t.Errorf("CancelJob: %v", err)
	}
}

func TestBadAPIKeyIsRejected(t *testing.T) {
	m := startMock(t)
	c := liveClient(t, m.base)
	c.APIKey = "wrong-key"
	if _, err := c.ListJobs(context.Background(), 1, ""); err == nil {
		t.Fatal("expected an auth error")
	} else if _, ok := err.(*AuthenticationError); !ok {
		t.Errorf("got %T, want *AuthenticationError", err)
	}
}

// ---------------------------------------------------------------------------
// Failures and fallbacks
// ---------------------------------------------------------------------------

func TestFailedJobSurfacesStepAndReason(t *testing.T) {
	m := startMock(t, "--fail-at", "gpu_timestamps")
	_, err := liveClient(t, m.base).TranscribeBytes(
		context.Background(), audioBytes(), TranscribeOptions{}, nil)
	if err == nil {
		t.Fatal("expected the job failure to surface")
	}
	if !strings.Contains(err.Error(), "gpu_timestamps") {
		t.Errorf("error does not name the failing step: %v", err)
	}
}

func TestRefusedStreamFallsBackToPollingQuickly(t *testing.T) {
	// A proxy that will not pass text/event-stream answers 503 forever. The
	// client must still deliver the transcript, and must not burn the full
	// sseMaxReconnects ladder to work that out.
	m := startMock(t, "--stream-status", "503")
	start := time.Now()
	got, err := liveClient(t, m.base).TranscribeBytes(
		context.Background(), audioBytes(), TranscribeOptions{}, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if got.Text() == "" {
		t.Error("empty transcript")
	}
	ceiling := time.Duration(sseMaxStatusRefusals) * sseReconnectDelay * 2
	if elapsed > ceiling {
		t.Errorf("refused stream took %s to fall back; want under %s "+
			"(the full ladder would be %s)", elapsed, ceiling,
			time.Duration(sseMaxReconnects)*sseReconnectDelay)
	}
}

// ---------------------------------------------------------------------------
// Webhooks
// ---------------------------------------------------------------------------

func TestWebhookIsDelivered(t *testing.T) {
	type delivery struct {
		body    []byte
		headers http.Header
	}
	got := make(chan delivery, 1)

	port := freePort(t)
	srv := &http.Server{
		Addr: fmt.Sprintf("127.0.0.1:%d", port),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			buf := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(buf)
			select {
			case got <- delivery{body: buf, headers: r.Header.Clone()}:
			default:
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		}),
	}
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() { _ = srv.Close() })
	time.Sleep(200 * time.Millisecond)

	m := startMock(t, "--webhook-secret", "whsec_go_functional")
	hook := fmt.Sprintf("http://127.0.0.1:%d/webhooks", port)
	id, err := liveClient(t, m.base).SubmitURL(context.Background(),
		"https://example.com/a.mp3", TranscribeOptions{CallbackURL: hook})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	select {
	case d := <-got:
		var event map[string]any
		if err := json.Unmarshal(d.body, &event); err != nil {
			t.Fatalf("webhook body is not JSON: %v (%q)", err, d.body)
		}
		if event["job_id"] != id {
			t.Errorf("job_id = %v, want %s", event["job_id"], id)
		}
		if event["status"] != "completed" {
			t.Errorf("status = %v", event["status"])
		}
		// Verified live: a completion carries the presigned download_url, so a
		// receiver needs no second call.
		if s, _ := event["download_url"].(string); !strings.HasPrefix(s, "http") {
			t.Errorf("no usable download_url in the payload: %v", event["download_url"])
		}
		if d.headers.Get("X-SR-Event") != "completed" {
			t.Errorf("X-SR-Event = %q", d.headers.Get("X-SR-Event"))
		}
		if d.headers.Get("X-SR-Delivery") == "" {
			t.Error("no X-SR-Delivery header")
		}
		if sig := d.headers.Get("X-SR-Signature"); !strings.HasPrefix(sig, "sha256=") {
			t.Errorf("X-SR-Signature = %q, want sha256=...", sig)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("no webhook arrived")
	}
}
