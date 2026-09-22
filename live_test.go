package stt

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Tests that talk to the REAL Speech Revolutions API.
//
// Everything else in this package runs against mock_api.py, which is a MODEL of
// the contract written by reading the server. A model can be wrong in the same
// way the client is wrong, and then the suite is green while production is
// broken. These close that gap for Go specifically: until they existed, the Go
// client had never once been pointed at the real API, and the mock was the only
// thing claiming it worked.
//
// OPT-IN, because they create real jobs on a real account and cost real money
// (a few seconds of audio each, so fractions of a cent):
//
//	SR_LIVE=1 SPEECHREVOLUTIONS_API_KEY=stt_... go test ./... -run Live -v
//
// Override the clip with SR_LIVE_AUDIO=/path/to/clip.mp3.

func liveAPIClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("SR_LIVE") != "1" {
		t.Skip("live API tests are opt-in: set SR_LIVE=1 (creates real, billable jobs)")
	}
	if os.Getenv("SPEECHREVOLUTIONS_API_KEY") == "" {
		t.Skip("SPEECHREVOLUTIONS_API_KEY is not set")
	}
	c, err := NewClient("")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.Timeout = 15 * time.Minute
	return c
}

// liveAudio returns the shortest clip available: these create real, billed
// jobs, so a 2MB clip beats a 68-minute one.
func liveAudio(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("SR_LIVE_AUDIO"); p != "" {
		return p
	}
	wd, _ := os.Getwd()
	repo := filepath.Join(wd, "..")
	for _, p := range []string{
		filepath.Join(repo, "qa-audio", "ru_uk_segment.mp3"),
		filepath.Join(repo, "qa-audio", "crawl11.mp3"),
	} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	t.Skip("no test audio found; set SR_LIVE_AUDIO")
	return ""
}

func liveCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// ---------------------------------------------------------------------------
// Read-only: no jobs created, no cost
// ---------------------------------------------------------------------------

func TestLiveListJobsShape(t *testing.T) {
	c, ctx := liveAPIClient(t), liveCtx(t)

	page, err := c.ListJobs(ctx, 3, "")
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	for _, j := range page.Jobs {
		if j.JobID == "" {
			t.Error("a job summary came back with no job_id")
		}
		// Production sends {job_id, created_at} and no status. The mock used to
		// invent one; this is what stops that happening again.
		if j.CreatedAt == "" {
			t.Errorf("job %s has no created_at", j.JobID)
		}
	}
}

func TestLiveListJobsPaginatesWithoutDuplicates(t *testing.T) {
	c, ctx := liveAPIClient(t), liveCtx(t)

	seen := map[string]bool{}
	before := ""
	for i := 0; i < 3; i++ {
		page, err := c.ListJobs(ctx, 2, before)
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		for _, j := range page.Jobs {
			if seen[j.JobID] {
				t.Fatalf("pagination returned %s twice", j.JobID)
			}
			seen[j.JobID] = true
		}
		if before = page.NextBefore; before == "" {
			break
		}
	}
}

func TestLiveBadKeyIsRejected(t *testing.T) {
	_ = liveAPIClient(t) // gate on SR_LIVE only
	c, err := NewClient("stt_definitely_not_a_real_key")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := c.ListJobs(liveCtx(t), 1, ""); err == nil {
		t.Fatal("a bogus key was accepted")
	}
}

// ---------------------------------------------------------------------------
// Billable: each of these transcribes a real clip
// ---------------------------------------------------------------------------

func TestLiveTranscribeFromPath(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	result, err := c.Transcribe(ctx, audio, TranscribeOptions{
		SpeakerLabels: Bool(true),
	}, nil)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}

	if strings.TrimSpace(result.Text()) == "" {
		t.Error("the real API returned an empty transcript")
	}
	if len(result.Words) == 0 {
		t.Error("no words came back; WordTimestamps defaults to on")
	}
}

// Live progress is a headline feature, so prove it actually arrives — but only
// on a file long enough to emit intermediate events. A short clip finishes
// before the first one is sent (measured: an 11-minute file transcribed in 10s
// produces none, in Go and in Python alike), so asserting on the default clip
// tests the clock, not the client.
func TestLiveProgressFiresForALongFile(t *testing.T) {
	c, ctx := liveAPIClient(t), liveCtx(t)

	long := os.Getenv("SR_LIVE_LONG_AUDIO")
	if long == "" {
		t.Skip("set SR_LIVE_LONG_AUDIO to a file of 20 minutes or more")
	}
	if _, err := os.Stat(long); err != nil {
		t.Skipf("SR_LIVE_LONG_AUDIO does not exist: %v", err)
	}

	var steps []string
	result, err := c.Transcribe(ctx, long, TranscribeOptions{}, func(e ProgressEvent) {
		steps = append(steps, e.Step)
	})
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if strings.TrimSpace(result.Text()) == "" {
		t.Error("empty transcript")
	}
	if len(steps) < 3 {
		t.Fatalf("expected several progress events, got %d: %v", len(steps), steps)
	}

	var sawChunk bool
	for _, s := range steps {
		if strings.HasPrefix(s, "chunk:") {
			sawChunk = true
		}
	}
	if !sawChunk {
		t.Errorf("no chunk steps in %v", steps)
	}
	t.Logf("progress steps: %v", steps)
}

func TestLiveTranscribeBytes(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	data, err := os.ReadFile(audio)
	if err != nil {
		t.Fatalf("read audio: %v", err)
	}
	result, err := c.TranscribeBytes(ctx, data, TranscribeOptions{}, nil)
	if err != nil {
		t.Fatalf("TranscribeBytes: %v", err)
	}
	if strings.TrimSpace(result.Text()) == "" {
		t.Error("empty transcript from raw bytes")
	}
}

func TestLiveSubmitThenPollToCompletion(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	jobID, err := c.Submit(ctx, audio, TranscribeOptions{})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(jobID) != 36 {
		t.Errorf("job id %q is not a UUID", jobID)
	}

	// CheckFailed on a job that has not failed.
	flags, err := c.CheckFailed(ctx, []string{jobID})
	if err != nil {
		t.Fatalf("CheckFailed: %v", err)
	}
	if len(flags) != 1 || flags[0] {
		t.Errorf("CheckFailed said a fresh job had already failed: %v", flags)
	}

	deadline := time.Now().Add(10 * time.Minute)
	for {
		status, err := c.GetJobStatus(ctx, jobID)
		if err != nil {
			t.Fatalf("GetJobStatus: %v", err)
		}
		if status.IsCompleted() {
			if status.DownloadURL == "" {
				t.Error("a completed job carried no download_url")
			}
			result, err := c.GetTranscript(ctx, jobID, OutputJSON)
			if err != nil {
				t.Fatalf("GetTranscript: %v", err)
			}
			if strings.TrimSpace(result.Text()) == "" {
				t.Error("empty transcript from GetTranscript")
			}
			// DownloadResult against the same presigned URL.
			raw, err := c.DownloadResult(ctx, status.DownloadURL)
			if err != nil {
				t.Fatalf("DownloadResult: %v", err)
			}
			if len(raw) == 0 {
				t.Error("DownloadResult returned no bytes")
			}
			return
		}
		if status.IsFailed() {
			t.Fatalf("job %s failed at %s: %s", jobID, status.FailedStage, status.Reason)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s did not finish within 10 minutes", jobID)
		}
		time.Sleep(3 * time.Second)
	}
}

func TestLiveSRTOutput(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	result, err := c.Transcribe(ctx, audio, TranscribeOptions{OutputType: OutputSRT}, nil)
	if err != nil {
		t.Fatalf("Transcribe(srt): %v", err)
	}
	if !strings.Contains(result.Text(), "-->") {
		t.Errorf("SRT output has no cue arrow; got %.120q", result.Text())
	}
}

// The full upload flow the convenience methods wrap: create, upload, complete,
// wait, download. Nothing else exercises these against the real API.
func TestLiveRawUploadFlow(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	data, err := os.ReadFile(audio)
	if err != nil {
		t.Fatalf("read audio: %v", err)
	}

	job, err := c.CreateUploadJob(ctx, len(data), TranscribeOptions{})
	if err != nil {
		t.Fatalf("CreateUploadJob: %v", err)
	}
	if job.JobID == "" || job.UploadURL == "" {
		t.Fatalf("CreateUploadJob returned %+v", job)
	}

	if err := c.UploadAudio(ctx, job.UploadURL, data, job.JobID); err != nil {
		t.Fatalf("UploadAudio: %v", err)
	}
	if err := c.TouchUploadProgress(ctx, job.JobID); err != nil {
		t.Fatalf("TouchUploadProgress: %v", err)
	}
	if err := c.CompleteUpload(ctx, job.JobID); err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}

	content, _, err := c.WaitForResult(ctx, job.JobID, job.DownloadURL, nil)
	if err != nil {
		t.Fatalf("WaitForResult: %v", err)
	}
	if len(content) == 0 {
		t.Error("WaitForResult returned no bytes")
	}
}

func TestLiveCancelJob(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	data, err := os.ReadFile(audio)
	if err != nil {
		t.Fatalf("read audio: %v", err)
	}
	// Cancel before completing the upload, so nothing is ever transcribed and
	// the job costs nothing.
	job, err := c.CreateUploadJob(ctx, len(data), TranscribeOptions{})
	if err != nil {
		t.Fatalf("CreateUploadJob: %v", err)
	}
	if err := c.CancelJob(ctx, job.JobID); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Webhooks
//
// Needs a PUBLIC callback URL: the pipeline resolves the callback host and
// refuses anything that is not globally routable, so localhost is rejected
// before a request is made. Point SR_LIVE_WEBHOOK_BASE at a tunnel:
//
//	SR_LIVE=1 SR_LIVE_WEBHOOK_BASE=https://xyz.trycloudflare.com \
//	    SR_LIVE_WEBHOOK_PORT=8799 go test ./... -run LiveWebhook -v
// ---------------------------------------------------------------------------

func TestLiveWebhookIsDeliveredByTheRealPipeline(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	base := os.Getenv("SR_LIVE_WEBHOOK_BASE")
	if base == "" {
		t.Skip("set SR_LIVE_WEBHOOK_BASE to a public tunnel URL " +
			"(the pipeline refuses non-public callback hosts)")
	}
	port := os.Getenv("SR_LIVE_WEBHOOK_PORT")
	if port == "" {
		port = "8799"
	}

	type delivery struct {
		raw     []byte
		headers http.Header
		path    string
	}
	got := make(chan delivery, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		select {
		case got <- delivery{raw: raw, headers: r.Header.Clone(), path: r.URL.Path}:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	})
	srv := &http.Server{Addr: "127.0.0.1:" + port, Handler: mux}
	go srv.ListenAndServe()
	defer srv.Shutdown(context.Background())

	callback := strings.TrimRight(base, "/") + "/webhooks/speechrevolutions"
	jobID, err := c.Submit(ctx, audio, TranscribeOptions{CallbackURL: callback})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	t.Logf("submitted %s, awaiting callback at %s", jobID, callback)

	var d delivery
	select {
	case d = <-got:
	case <-time.After(15 * time.Minute):
		t.Fatal("no webhook arrived within 15 minutes")
	}

	var event struct {
		JobID       string `json:"job_id"`
		Status      string `json:"status"`
		DownloadURL string `json:"download_url"`
	}
	if err := json.Unmarshal(d.raw, &event); err != nil {
		t.Fatalf("webhook body is not JSON: %v (%.200q)", err, d.raw)
	}

	if event.JobID != jobID {
		t.Errorf("webhook was for job %s, expected %s", event.JobID, jobID)
	}
	if event.Status != "completed" && event.Status != "failed" {
		t.Errorf("unexpected status %q", event.Status)
	}
	if !strings.HasSuffix(d.path, "/webhooks/speechrevolutions") {
		t.Errorf("delivered to %s", d.path)
	}

	// Headers the documented receivers rely on.
	if h := d.headers.Get("X-SR-Event"); h != event.Status {
		t.Errorf("X-SR-Event is %q, body status is %q", h, event.Status)
	}
	if d.headers.Get("X-SR-Delivery") == "" {
		t.Error("no X-SR-Delivery id")
	}
	if ua := d.headers.Get("User-Agent"); !strings.HasPrefix(ua, "SpeechRevolutions-Webhook/") {
		t.Errorf("unexpected webhook User-Agent %q", ua)
	}

	// Absent means the secret is unset server-side, which is worth reporting
	// rather than silently passing.
	if sig := d.headers.Get("X-SR-Signature"); sig != "" {
		if !strings.HasPrefix(sig, "sha256=") {
			t.Errorf("unexpected signature format: %s", sig)
		} else {
			t.Log("signature present and well-formed")
		}
	} else {
		t.Log("NOTE: no X-SR-Signature — webhook_signing_secret is unset server-side")
	}
}

// ---------------------------------------------------------------------------
// Ingestion paths, output types, options and transforms
//
// The suite above covers the path most callers take. These cover the rest of
// the published surface, so that "tested live" means every exported method has
// actually been run against production rather than only the common ones.
//
// The URL tests need a PUBLICLY reachable audio file, because the platform
// fetches it server-side: SR_LIVE_AUDIO_URL=https://.../clip.mp3
// ---------------------------------------------------------------------------

func liveAudioURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("SR_LIVE_AUDIO_URL")
	if u == "" {
		t.Skip("set SR_LIVE_AUDIO_URL to a publicly reachable audio file " +
			"(the platform fetches it server-side, so a local path will not do)")
	}
	return u
}

func TestLiveTranscribeURL(t *testing.T) {
	c, ctx := liveAPIClient(t), liveCtx(t)
	url := liveAudioURL(t)

	// The explicit alias, and the auto-detection in Transcribe, must both hand
	// the URL to the server rather than download it here.
	result, err := c.TranscribeURL(ctx, url, TranscribeOptions{}, nil)
	if err != nil {
		t.Fatalf("TranscribeURL: %v", err)
	}
	if strings.TrimSpace(result.Text()) == "" {
		t.Error("empty transcript from TranscribeURL")
	}

	auto, err := c.Transcribe(ctx, url, TranscribeOptions{}, nil)
	if err != nil {
		t.Fatalf("Transcribe(url): %v", err)
	}
	if strings.TrimSpace(auto.Text()) == "" {
		t.Error("empty transcript from auto-detected URL")
	}
}

func TestLiveTranscribeFile(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	result, err := c.TranscribeFile(ctx, audio, TranscribeOptions{}, nil)
	if err != nil {
		t.Fatalf("TranscribeFile: %v", err)
	}
	if strings.TrimSpace(result.Text()) == "" {
		t.Error("empty transcript from TranscribeFile")
	}
}

func TestLiveSubmitURLAndSubmitBytes(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	data, err := os.ReadFile(audio)
	if err != nil {
		t.Fatalf("read audio: %v", err)
	}
	byteJob, err := c.SubmitBytes(ctx, data, TranscribeOptions{})
	if err != nil {
		t.Fatalf("SubmitBytes: %v", err)
	}
	if len(byteJob) != 36 {
		t.Errorf("SubmitBytes returned %q, not a UUID", byteJob)
	}

	if u := os.Getenv("SR_LIVE_AUDIO_URL"); u != "" {
		urlJob, err := c.SubmitURL(ctx, u, TranscribeOptions{})
		if err != nil {
			t.Fatalf("SubmitURL: %v", err)
		}
		if len(urlJob) != 36 {
			t.Errorf("SubmitURL returned %q, not a UUID", urlJob)
		}
	}
}

// Every output type the API advertises, against the real renderer. Only SRT had
// ever been checked live, and docx/pdf go through a different server-side path
// than the text formats.
func TestLiveEveryOutputType(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	for _, tc := range []struct {
		out      OutputType
		contains string // expected in the decoded text, "" for binary formats
	}{
		{OutputJSON, ""},
		{OutputTXT, ""},
		{OutputSRT, "-->"},
		{OutputVTT, "-->"},
		{OutputDOCX, ""},
		{OutputPDF, ""},
	} {
		tc := tc
		t.Run(string(tc.out), func(t *testing.T) {
			result, err := c.Transcribe(ctx, audio, TranscribeOptions{OutputType: tc.out}, nil)
			if err != nil {
				t.Fatalf("Transcribe(%s): %v", tc.out, err)
			}
			if len(result.Content) == 0 {
				t.Fatalf("%s came back with no bytes", tc.out)
			}
			if tc.contains != "" && !strings.Contains(result.Text(), tc.contains) {
				t.Errorf("%s does not contain %q: %.120q", tc.out, tc.contains, result.Text())
			}
			if tc.out == OutputDOCX && !strings.HasPrefix(string(result.Content), "PK") {
				t.Errorf("docx is not a zip container: %.8q", result.Content)
			}
			if tc.out == OutputPDF && !strings.HasPrefix(string(result.Content), "%PDF") {
				t.Errorf("pdf lacks the %%PDF header: %.8q", result.Content)
			}
		})
	}
}

// The transcription options, exercised against the real model rather than a
// mock that accepts anything.
func TestLiveTranscribeOptions(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	t.Run("diarize alias", func(t *testing.T) {
		result, err := c.Transcribe(ctx, audio, TranscribeOptions{
			Diarize: Bool(true), // Deepgram-compatible alias for SpeakerLabels
		}, nil)
		if err != nil {
			t.Fatalf("Transcribe(diarize): %v", err)
		}
		if len(result.Utterances) == 0 {
			t.Error("diarize produced no utterances")
		}
	})

	t.Run("custom vocabulary", func(t *testing.T) {
		result, err := c.Transcribe(ctx, audio, TranscribeOptions{
			CustomVocabulary: []string{"Kyiv", "Dnipro"},
		}, nil)
		if err != nil {
			t.Fatalf("Transcribe(custom_vocabulary): %v", err)
		}
		if strings.TrimSpace(result.Text()) == "" {
			t.Error("empty transcript with custom vocabulary")
		}
	})

	t.Run("word timestamps off", func(t *testing.T) {
		result, err := c.Transcribe(ctx, audio, TranscribeOptions{
			WordTimestamps: Bool(false),
		}, nil)
		if err != nil {
			t.Fatalf("Transcribe(word_timestamps=false): %v", err)
		}
		if strings.TrimSpace(result.Text()) == "" {
			t.Error("empty transcript with word timestamps off")
		}
	})
}

// The single-shot presigned PUT, rather than the multipart flow Transcribe
// prefers by default. Both reach the same endpoint set, and only one of them
// was being exercised.
func TestLiveSingleShotUpload(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)
	c.Multipart = false

	result, err := c.Transcribe(ctx, audio, TranscribeOptions{}, nil)
	if err != nil {
		t.Fatalf("Transcribe with Multipart=false: %v", err)
	}
	if strings.TrimSpace(result.Text()) == "" {
		t.Error("empty transcript from the single-shot upload path")
	}
}

// The transforms, run over a real response rather than a fixture. A mock can
// hand back a shape these happen to survive; production is the real input.
func TestLiveTranscriptTransforms(t *testing.T) {
	c, ctx, audio := liveAPIClient(t), liveCtx(t), liveAudio(t)

	result, err := c.Transcribe(ctx, audio, TranscribeOptions{
		SpeakerLabels: Bool(true),
	}, nil)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}

	d := result.ToDict()
	for _, k := range []string{"id", "status", "text", "words", "utterances", "output_type"} {
		if _, ok := d[k]; !ok {
			t.Errorf("ToDict is missing %q", k)
		}
	}

	dg := result.ToDeepgram()
	if _, ok := dg["results"]; !ok {
		t.Errorf("ToDeepgram has no results key: %v", keysOf(dg))
	}

	dir := t.TempDir()
	path, err := result.Save(filepath.Join(dir, "out"))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if !strings.HasSuffix(path, ".json") {
		t.Errorf("Save inferred %q, expected a .json extension", path)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() == 0 {
		t.Errorf("Save wrote nothing to %s (%v)", path, err)
	}

	if result.TranscriptText() != result.Text() {
		t.Error("TranscriptText and Text disagree")
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
