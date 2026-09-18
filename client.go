package stt

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultBaseURL = "https://api.speechrevolutions.com"

	uploadProgressInterval = 10 * time.Second
	uploadMaxAttempts      = 4
	uploadBaseDelay        = 1 * time.Second

	sseMaxReconnects  = 10
	sseReconnectDelay = 3 * time.Second

	// How many times to retry a stream endpoint that answered with a NON-2xx
	// status, as opposed to one whose connection dropped.
	//
	// The two look the same to the reconnect loop and are not the same thing.
	// A drop is transient. A non-2xx is a refusal: a proxy or load balancer
	// that does not pass text/event-stream answers every attempt identically,
	// forever, so the full ladder just burns 30s before falling back to
	// polling — on every job.
	sseMaxStatusRefusals = 2
	pollInterval         = 5 * time.Second

	// Per-request deadlines, applied on top of the caller's context.
	apiTimeout      = 30 * time.Second
	downloadTimeout = 60 * time.Second
	uploadTimeout   = 300 * time.Second

	defaultMaxRetries   = 3
	defaultRetryBackoff = 500 * time.Millisecond
	retryBackoffMax     = 30 * time.Second
)

// Statuses worth a second attempt on a JSON API request.
var retryStatusCodes = map[int]bool{429: true, 500: true, 502: true, 503: true, 504: true}

// Endpoints that CREATE a job, and so are not safe to blindly retry.
//
// A job is created the moment the server handles one of these; the response
// carrying the job ID back is what can be lost. Retrying after the request may
// have arrived creates a SECOND job for the same audio — two transcripts, two
// charges — and the caller never learns about the orphan. The API has no
// idempotency key, so the only safe rule is to retry these solely when the
// request provably never reached the server.
//
// Every other endpoint either reads, or acts on a job ID the caller already
// holds, and stays fully retryable.
var jobCreatingPaths = map[string]bool{
	"/api/v1/upload":                  true,
	"/api/v1/upload/multipart/create": true,
}

func createsJob(path string) bool {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	return jobCreatingPaths[strings.TrimRight(path, "/")]
}

// neverReachedServer reports whether err proves the request never got to the
// server, so retrying it cannot duplicate work. A DNS failure or a dial error
// both mean no connection was ever established; anything later (a reset
// mid-flight, a response-read timeout) is ambiguous.
func neverReachedServer(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

// Client talks to the Speech Revolutions STT API.
type Client struct {
	APIKey  string
	BaseURL string
	// Timeout bounds a whole transcription wait (SSE + polling). Individual
	// HTTP calls have their own shorter deadlines.
	Timeout time.Duration
	HTTP    *http.Client
	// Multipart prefers S3 multipart uploads and falls back to a single presigned
	// PUT if the server has multipart disabled or a multipart upload fails
	// mid-flight. Defaults to true (set by NewClient).
	Multipart bool
	// MaxRetries is the number of extra attempts for a JSON API request that
	// fails to connect or returns 429/5xx. Uploads and SSE have their own loops.
	MaxRetries int
	// RetryBackoff is the first retry delay; it doubles per attempt, capped at 30s.
	RetryBackoff time.Duration
}

// errMultipartUnavailable signals that a multipart upload should fall back to
// the single-shot path (server has multipart disabled, or a mid-flight failure).
var errMultipartUnavailable = errors.New("multipart unavailable")

// resolveBaseURL picks the API host: an explicit value, then the environment,
// then production.
//
// Symmetric with the API key — if a caller can supply a key from the
// environment, they can point it at an environment too. Needed for staging, for
// an egress proxy or gateway, and for running any published example against
// something that is not production.
func resolveBaseURL(baseURL string) string {
	if baseURL == "" {
		baseURL = os.Getenv("SPEECHREVOLUTIONS_BASE_URL")
	}
	if baseURL == "" {
		baseURL = os.Getenv("STT_BASE_URL")
	}
	if baseURL == "" {
		return defaultBaseURL
	}
	return strings.TrimRight(baseURL, "/")
}

// NewClient creates a Client. If apiKey is empty, reads
// SPEECHREVOLUTIONS_API_KEY or STT_API_KEY from the environment.
func NewClient(apiKey string) (*Client, error) {
	if apiKey == "" {
		apiKey = os.Getenv("SPEECHREVOLUTIONS_API_KEY")
	}
	if apiKey == "" {
		apiKey = os.Getenv("STT_API_KEY")
	}
	if apiKey == "" {
		return nil, authErr("api_key is required (pass apiKey or set SPEECHREVOLUTIONS_API_KEY / STT_API_KEY)")
	}
	return &Client{
		APIKey:       apiKey,
		BaseURL:      resolveBaseURL(""),
		Timeout:      600 * time.Second,
		HTTP:         &http.Client{},
		Multipart:    true,
		MaxRetries:   defaultMaxRetries,
		RetryBackoff: defaultRetryBackoff,
	}, nil
}

// Transcribe uploads audio, waits for completion, and returns a Transcript.
// audioPath may be a local filesystem path or an http(s) URL; a URL is handed
// to the platform to fetch, so nothing is uploaded from here.
func (c *Client) Transcribe(ctx context.Context, audioPath string, opts TranscribeOptions, onProgress ProgressFunc) (*Transcript, error) {
	if isURL(audioPath) {
		return c.TranscribeURL(ctx, audioPath, opts, onProgress)
	}
	data, err := os.ReadFile(audioPath)
	if err != nil {
		return nil, err
	}
	return c.TranscribeBytes(ctx, data, opts, onProgress)
}

// TranscribeURL transcribes audio the platform fetches from a public http(s)
// URL, then waits for the result.
func (c *Client) TranscribeURL(ctx context.Context, audioURL string, opts TranscribeOptions, onProgress ProgressFunc) (*Transcript, error) {
	opts = opts.withDefaults()
	jobID, jobDownloadURL, err := c.submitURL(ctx, audioURL, opts)
	if err != nil {
		return nil, err
	}
	return c.awaitTranscript(ctx, jobID, jobDownloadURL, opts, onProgress)
}

// TranscribeFile is an alias for Transcribe with a local path.
func (c *Client) TranscribeFile(ctx context.Context, path string, opts TranscribeOptions, onProgress ProgressFunc) (*Transcript, error) {
	return c.Transcribe(ctx, path, opts, onProgress)
}

// TranscribeBytes is like Transcribe but accepts raw audio bytes.
func (c *Client) TranscribeBytes(ctx context.Context, data []byte, opts TranscribeOptions, onProgress ProgressFunc) (*Transcript, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("audio is empty")
	}
	opts = opts.withDefaults()

	uploadCb, uploadBar := resolveProgress(opts.OnUploadProgress, opts.Progress, "Uploading", true)
	jobID, jobDownloadURL, err := c.ingestUpload(ctx, data, opts, byteProgressAdapter(uploadCb))
	uploadBar.close()
	if err != nil {
		return nil, err
	}
	return c.awaitTranscript(ctx, jobID, jobDownloadURL, opts, onProgress)
}

// awaitTranscript waits out the transcription phase and parses the result.
func (c *Client) awaitTranscript(ctx context.Context, jobID, jobDownloadURL string, opts TranscribeOptions, onProgress ProgressFunc) (*Transcript, error) {
	progressCb, bar := resolveProgress(onProgress, opts.Progress, "Transcribing", false)
	content, downloadURL, err := c.WaitForResult(ctx, jobID, jobDownloadURL, progressCb)
	bar.close()
	if err != nil {
		return nil, err
	}
	return parseTranscript(jobID, content, opts.OutputType, downloadURL), nil
}

// Submit uploads and enqueues a job, returning its job_id WITHOUT waiting for
// the result. Collect it later via a webhook (opts.CallbackURL) or by polling
// GetJobStatus / GetTranscript. Ideal for batch workloads. audioPath may be a
// local path or an http(s) URL.
func (c *Client) Submit(ctx context.Context, audioPath string, opts TranscribeOptions) (string, error) {
	if isURL(audioPath) {
		return c.SubmitURL(ctx, audioPath, opts)
	}
	data, err := os.ReadFile(audioPath)
	if err != nil {
		return "", err
	}
	return c.SubmitBytes(ctx, data, opts)
}

// SubmitURL enqueues audio the platform fetches from a public http(s) URL.
func (c *Client) SubmitURL(ctx context.Context, audioURL string, opts TranscribeOptions) (string, error) {
	jobID, _, err := c.submitURL(ctx, audioURL, opts.withDefaults())
	return jobID, err
}

// SubmitBytes is like Submit but accepts raw audio bytes.
func (c *Client) SubmitBytes(ctx context.Context, data []byte, opts TranscribeOptions) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("audio is empty")
	}
	opts = opts.withDefaults()

	uploadCb, uploadBar := resolveProgress(opts.OnUploadProgress, opts.Progress, "Uploading", true)
	jobID, _, err := c.ingestUpload(ctx, data, opts, byteProgressAdapter(uploadCb))
	uploadBar.close()
	if err != nil {
		return "", err
	}
	return jobID, nil
}

type uploadRequest struct {
	FileSize         int      `json:"file_size,omitempty"`
	AudioURL         string   `json:"audio_url,omitempty"`
	OutputType       string   `json:"output_type"`
	WordTimestamps   bool     `json:"word_timestamps"`
	SpeakerLabels    bool     `json:"speaker_labels"`
	NLTK             bool     `json:"nltk"`
	Tier             string   `json:"tier"`
	CustomVocabulary []string `json:"custom_vocabulary,omitempty"`
	CallbackURL      string   `json:"callback_url,omitempty"`
}

type uploadResponse struct {
	JobID       string `json:"job_id"`
	UploadURL   string `json:"upload_url"`
	DownloadURL string `json:"download_url"`
	ContentType string `json:"content_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type multipartCreateResponse struct {
	JobID       string `json:"job_id"`
	UploadID    string `json:"upload_id"`
	DownloadURL string `json:"download_url"`
	PartSize    int    `json:"part_size"`
	NumParts    int    `json:"num_parts"`
	Parts       []struct {
		PartNumber int    `json:"part_number"`
		URL        string `json:"url"`
	} `json:"parts"`
}

// uploadBody is the JSON shared by the single-shot and multipart create endpoints.
func uploadBody(fileSize int, opts TranscribeOptions) uploadRequest {
	opts = opts.withDefaults()
	return uploadRequest{
		FileSize:         fileSize,
		OutputType:       string(opts.OutputType),
		WordTimestamps:   *opts.WordTimestamps,
		SpeakerLabels:    *opts.SpeakerLabels,
		NLTK:             *opts.NLTK,
		Tier:             string(*opts.Tier),
		CustomVocabulary: opts.CustomVocabulary,
		CallbackURL:      opts.CallbackURL,
	}
}

// submitURL registers a job the platform fetches itself, returning
// (jobID, downloadURL). No bytes leave this process.
func (c *Client) submitURL(ctx context.Context, audioURL string, opts TranscribeOptions) (string, string, error) {
	body := uploadBody(0, opts)
	body.AudioURL = audioURL

	var resp uploadResponse
	if err := c.apiRequest(ctx, "POST", "/api/v1/upload", body, &resp); err != nil {
		return "", "", err
	}
	return resp.JobID, resp.DownloadURL, nil
}

// ingestUpload gets audio into the platform and returns (jobID, downloadURL).
// It prefers a multipart upload (when Multipart is set) and falls back to a
// single presigned PUT if the server has multipart disabled or a multipart
// upload fails mid-flight.
func (c *Client) ingestUpload(ctx context.Context, data []byte, opts TranscribeOptions, byteCb func(sent, total int)) (string, string, error) {
	if c.Multipart {
		jobID, downloadURL, err := c.uploadMultipart(ctx, data, opts, byteCb)
		if err == nil {
			return jobID, downloadURL, nil
		}
		if !errors.Is(err, errMultipartUnavailable) {
			return "", "", err
		}
		// multipart unavailable — fall through to the single-shot path
	}
	job, err := c.CreateUploadJob(ctx, len(data), opts)
	if err != nil {
		return "", "", err
	}
	if err := c.uploadAudio(ctx, job.UploadURL, data, job.JobID, byteCb); err != nil {
		return "", "", err
	}
	if err := c.CompleteUpload(ctx, job.JobID); err != nil {
		return "", "", err
	}
	return job.JobID, job.DownloadURL, nil
}

// uploadMultipart runs the S3 multipart flow: create -> PUT each part ->
// complete. It returns an error wrapping errMultipartUnavailable when the
// server has multipart disabled (404) or a mid-flight failure means we should
// retry via the single-shot path. Other errors (e.g. create 5xx) propagate.
func (c *Client) uploadMultipart(ctx context.Context, data []byte, opts TranscribeOptions, byteCb func(sent, total int)) (string, string, error) {
	var created multipartCreateResponse
	if err := c.apiRequest(ctx, "POST", "/api/v1/upload/multipart/create", uploadBody(len(data), opts), &created); err != nil {
		var nf *JobNotFoundError
		if errors.As(err, &nf) { // route returns 404 when multipart is disabled
			return "", "", fmt.Errorf("multipart disabled: %w", errMultipartUnavailable)
		}
		return "", "", err
	}

	completed := make([]completedPart, 0, len(created.Parts))
	uploaded := 0
	for _, p := range created.Parts {
		start := (p.PartNumber - 1) * created.PartSize
		end := start + created.PartSize
		if end > len(data) {
			end = len(data)
		}
		etag, err := c.putPart(ctx, p.URL, data[start:end])
		if err != nil {
			if ctx.Err() != nil {
				return "", "", ctx.Err()
			}
			c.abortMultipart(ctx, created.JobID)
			return "", "", fmt.Errorf("multipart part upload failed: %w", errMultipartUnavailable)
		}
		completed = append(completed, completedPart{PartNumber: p.PartNumber, ETag: etag})
		uploaded += end - start
		if byteCb != nil {
			byteCb(uploaded, len(data))
		}
	}

	if err := c.apiRequest(ctx, "POST", "/api/v1/upload/multipart/complete",
		map[string]any{"job_id": created.JobID, "parts": completed}, nil); err != nil {
		if ctx.Err() != nil {
			return "", "", ctx.Err()
		}
		c.abortMultipart(ctx, created.JobID)
		return "", "", fmt.Errorf("multipart complete failed: %w", errMultipartUnavailable)
	}
	return created.JobID, created.DownloadURL, nil
}

type completedPart struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
}

// putPart PUTs one part to its presigned URL and returns the S3 ETag.
func (c *Client) putPart(ctx context.Context, rawURL string, chunk []byte) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, uploadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "PUT", rawURL, bytes.NewReader(chunk))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(chunk))
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 200 && resp.StatusCode != 204 {
		return "", uploadErr(fmt.Sprintf("part upload failed (HTTP %d)", resp.StatusCode))
	}
	etag := resp.Header.Get("ETag") // http.Header.Get is case-insensitive
	if etag == "" {
		return "", uploadErr("part upload response missing ETag header")
	}
	return etag, nil
}

// abortMultipart best-effort discards an in-progress multipart upload. It runs
// on its own deadline so a cancelled parent context still cleans up.
func (c *Client) abortMultipart(ctx context.Context, jobID string) {
	if ctx.Err() != nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, apiTimeout)
	defer cancel()
	_ = c.apiRequest(ctx, "POST", "/api/v1/upload/multipart/abort", map[string]string{"job_id": jobID}, nil)
}

// CreateUploadJob calls POST /api/v1/upload.
func (c *Client) CreateUploadJob(ctx context.Context, fileSize int, opts TranscribeOptions) (*UploadJob, error) {
	var resp uploadResponse
	if err := c.apiRequest(ctx, "POST", "/api/v1/upload", uploadBody(fileSize, opts), &resp); err != nil {
		return nil, err
	}

	return &UploadJob{
		JobID:       resp.JobID,
		UploadURL:   resp.UploadURL,
		DownloadURL: resp.DownloadURL,
		ContentType: resp.ContentType,
		ExpiresIn:   resp.ExpiresIn,
	}, nil
}

// TouchUploadProgress calls POST /api/v1/upload/progress.
func (c *Client) TouchUploadProgress(ctx context.Context, jobID string) error {
	return c.apiRequest(ctx, "POST", "/api/v1/upload/progress", map[string]string{"job_id": jobID}, nil)
}

// UploadAudio streams bytes to the presigned URL, with progress heartbeats.
func (c *Client) UploadAudio(ctx context.Context, uploadURL string, data []byte, jobID string) error {
	return c.uploadAudio(ctx, uploadURL, data, jobID, nil)
}

// uploadAudio is UploadAudio with an optional byte-level progress callback
// (sent, total) reported as bytes are streamed to the presigned target.
func (c *Client) uploadAudio(ctx context.Context, uploadURL string, data []byte, jobID string, byteCb func(sent, total int)) error {
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	var wg sync.WaitGroup
	if jobID != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(uploadProgressInterval)
			defer t.Stop()
			for {
				select {
				case <-heartbeatCtx.Done():
					return
				case <-t.C:
					_ = c.TouchUploadProgress(heartbeatCtx, jobID)
				}
			}
		}()
	}
	defer func() {
		stopHeartbeat()
		wg.Wait()
	}()

	var lastErr error
	for attempt := 1; attempt <= uploadMaxAttempts; attempt++ {
		lastErr = c.putUpload(ctx, uploadURL, data, byteCb)
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < uploadMaxAttempts {
			if err := sleepCtx(ctx, uploadBaseDelay*time.Duration(1<<(attempt-1))); err != nil {
				return err
			}
		}
	}
	return uploadErr(fmt.Sprintf("Upload failed after %d attempts: %v", uploadMaxAttempts, lastErr))
}

// CompleteUpload calls POST /api/v1/upload/complete.
func (c *Client) CompleteUpload(ctx context.Context, jobID string) error {
	return c.apiRequest(ctx, "POST", "/api/v1/upload/complete", map[string]string{"job_id": jobID}, nil)
}

// WaitForResult waits via SSE (with polling fallback) and downloads the result.
func (c *Client) WaitForResult(ctx context.Context, jobID, downloadURL string, onProgress ProgressFunc) ([]byte, string, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 600 * time.Second
	}

	url, err := c.waitSSE(ctx, jobID, downloadURL, onProgress, timeout)
	if err != nil {
		return nil, "", err
	}
	if url == "" {
		content, err := c.waitPoll(ctx, jobID, downloadURL, timeout)
		return content, downloadURL, err
	}
	content, err := c.DownloadResult(ctx, url)
	return content, url, err
}

// DownloadResult GETs the result bytes from a download URL.
func (c *Client) DownloadResult(ctx context.Context, downloadURL string) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, downloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", downloadURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, apiErr(fmt.Sprintf("Download failed: %v", err), 0, "")
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, apiErr(fmt.Sprintf("Download failed (HTTP %d)", resp.StatusCode), resp.StatusCode, truncate(string(body), 300))
	}
	return body, nil
}

// CancelJob calls POST /api/v1/jobs/cancel.
func (c *Client) CancelJob(ctx context.Context, jobID string) error {
	return c.apiRequest(ctx, "POST", "/api/v1/jobs/cancel", map[string]string{"job_id": jobID}, nil)
}

// CheckFailed calls POST /api/v1/jobs/check-failed.
func (c *Client) CheckFailed(ctx context.Context, jobIDs []string) ([]bool, error) {
	var resp struct {
		FailedJobs []bool `json:"failed_jobs"`
	}
	if err := c.apiRequest(ctx, "POST", "/api/v1/jobs/check-failed", map[string]any{"job_ids": jobIDs}, &resp); err != nil {
		return nil, err
	}
	return resp.FailedJobs, nil
}

// GetJobStatus calls GET /api/v1/jobs/{id} for the current status (and a fresh
// download URL once the job has completed).
func (c *Client) GetJobStatus(ctx context.Context, jobID string) (*JobStatus, error) {
	var st JobStatus
	if err := c.apiRequest(ctx, "GET", "/api/v1/jobs/"+url.PathEscape(jobID), nil, &st); err != nil {
		return nil, err
	}
	if st.JobID == "" {
		st.JobID = jobID
	}
	return &st, nil
}

// GetTranscript fetches and parses a completed job's transcript by id. It
// returns a JobFailedError if the job failed, or an error if it is still
// processing (poll GetJobStatus for that case). Pass "" for outputType to
// default to JSON.
func (c *Client) GetTranscript(ctx context.Context, jobID string, outputType OutputType) (*Transcript, error) {
	if outputType == "" {
		outputType = OutputJSON
	}
	st, err := c.GetJobStatus(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if st.IsFailed() {
		return nil, jobFailedErr(st.FailedStage, st.Reason)
	}
	if !st.IsCompleted() || st.DownloadURL == "" {
		return nil, fmt.Errorf("job %s is not complete (status=%s)", jobID, st.Status)
	}
	content, err := c.DownloadResult(ctx, st.DownloadURL)
	if err != nil {
		return nil, err
	}
	return parseTranscript(jobID, content, outputType, st.DownloadURL), nil
}

// ListJobs calls GET /api/v1/jobs — the caller's most-recent jobs, newest
// first, cursor-paginated. Pass the returned NextBefore as before for the next
// page; limit <= 0 defaults to 50.
func (c *Client) ListJobs(ctx context.Context, limit int, before string) (*JobList, error) {
	if limit <= 0 {
		limit = 50
	}
	path := "/api/v1/jobs?limit=" + strconv.Itoa(limit)
	if before != "" {
		path += "&before=" + url.QueryEscape(before)
	}
	var out JobList
	if err := c.apiRequest(ctx, "GET", path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// apiRequest sends a JSON request, retrying transient failures and 429/5xx
// responses with exponential backoff (honoring Retry-After).
func (c *Client) apiRequest(ctx context.Context, method, path string, body any, out any) error {
	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = b
	}

	maxRetries := c.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}

	// Job-creating calls retry only when the request provably never landed;
	// anything else would risk a duplicate job and a duplicate charge.
	creating := createsJob(path)

	for attempt := 1; ; attempt++ {
		status, header, respBody, err := c.doJSON(ctx, method, path, payload)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if (!creating || neverReachedServer(err)) && attempt <= maxRetries {
				if serr := sleepCtx(ctx, c.retryDelay(attempt, 0, false)); serr != nil {
					return serr
				}
				continue
			}
			return apiErr(fmt.Sprintf("Cannot connect to %s: %v", c.BaseURL, err), 0, "")
		}

		// For a create, only 429 is safe to retry: the server refused it
		// outright, so no job exists. A 5xx may have created one before failing.
		if retryStatusCodes[status] && attempt <= maxRetries && (!creating || status == 429) {
			retryAfter, ok := parseRetryAfter(header.Get("Retry-After"))
			if serr := sleepCtx(ctx, c.retryDelay(attempt, retryAfter, ok)); serr != nil {
				return serr
			}
			continue
		}

		if err := raiseForStatus(status, header, respBody); err != nil {
			return err
		}
		if out == nil || len(respBody) == 0 {
			return nil
		}
		return json.Unmarshal(respBody, out)
	}
}

// doJSON performs one attempt and reads the whole response.
func (c *Client) doJSON(ctx context.Context, method, path string, payload []byte) (int, http.Header, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, apiTimeout)
	defer cancel()

	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, reader)
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, respBody, nil
}

func (c *Client) retryDelay(attempt int, retryAfter float64, hasRetryAfter bool) time.Duration {
	if hasRetryAfter {
		d := time.Duration(retryAfter * float64(time.Second))
		if d > retryBackoffMax {
			return retryBackoffMax
		}
		return d
	}
	base := c.RetryBackoff
	if base <= 0 {
		base = defaultRetryBackoff
	}
	d := time.Duration(math.Min(float64(base)*math.Pow(2, float64(attempt-1)), float64(retryBackoffMax)))
	return d
}

func raiseForStatus(status int, header http.Header, body []byte) error {
	if status == 200 || status == 204 {
		return nil
	}
	base := baseError{
		StatusCode: status,
		RequestID:  requestID(header),
		Body:       truncate(string(body), 300),
	}
	switch status {
	case 401:
		base.Message = "Unauthorized — check your API key"
		return &AuthenticationError{base}
	case 404:
		base.Message = "Job not found or upload session expired"
		return &JobNotFoundError{base}
	case 429:
		base.Message = "Rate limit exceeded — try again shortly"
		retryAfter, _ := parseRetryAfter(header.Get("Retry-After"))
		return &RateLimitError{baseError: base, RetryAfter: retryAfter}
	default:
		base.Message = fmt.Sprintf("Unexpected response (HTTP %d)", status)
		return &APIError{base}
	}
}

func (c *Client) putUpload(ctx context.Context, uploadURL string, data []byte, byteCb func(sent, total int)) error {
	ctx, cancel := context.WithTimeout(ctx, uploadTimeout)
	defer cancel()

	// Stream the body through a counting reader while keeping Content-Length
	// correct and explicit — a presigned S3 PUT rejects chunked transfer
	// encoding, so ContentLength must be set so net/http does NOT chunk.
	var body io.Reader = bytes.NewReader(data)
	if byteCb != nil {
		body = &progressReader{data: data, cb: byteCb}
	}
	req, err := http.NewRequestWithContext(ctx, "PUT", uploadURL, body)
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 204 {
		b, _ := io.ReadAll(resp.Body)
		return uploadErr(fmt.Sprintf("HTTP %d: %s", resp.StatusCode, truncate(string(b), 200)))
	}
	return nil
}

func (c *Client) waitSSE(ctx context.Context, jobID, fallback string, onProgress ProgressFunc, timeout time.Duration) (string, error) {
	start := time.Now()
	var lastEventID string
	reconnects := 0
	refusals := 0

	for {
		if time.Since(start) >= timeout {
			return "", timeoutErr(fmt.Sprintf("Timed out after %s waiting for job %s", timeout, jobID))
		}
		if reconnects > sseMaxReconnects {
			return "", nil // signal polling fallback
		}
		if reconnects > 0 {
			if err := sleepCtx(ctx, sseReconnectDelay); err != nil {
				return "", err
			}
		}

		outcome, url, newID, err := c.sseAttempt(ctx, jobID, start, timeout, lastEventID, fallback, onProgress)
		if newID != "" {
			lastEventID = newID
		}
		if err != nil {
			return "", err
		}
		switch outcome {
		case "done":
			if url == "" {
				url = fallback
			}
			return url, nil
		case "timeout":
			return "", timeoutErr(fmt.Sprintf("Timed out after %s waiting for job %s", timeout, jobID))
		case "refused":
			refusals++
			if refusals >= sseMaxStatusRefusals {
				return "", nil // the endpoint will not stream; poll instead
			}
			reconnects++
		case "reconnect":
			reconnects++
		}
	}
}

func (c *Client) sseAttempt(
	ctx context.Context,
	jobID string,
	start time.Time,
	timeout time.Duration,
	lastEventID string,
	fallback string,
	onProgress ProgressFunc,
) (outcome string, downloadURL string, newLastID string, err error) {
	newLastID = lastEventID
	if time.Since(start) >= timeout {
		return "timeout", "", newLastID, nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout-time.Since(start))
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", strings.TrimRight(c.BaseURL, "/")+"/api/v1/jobs/"+url.PathEscape(jobID)+"/stream", nil)
	if err != nil {
		return "reconnect", "", newLastID, nil
	}
	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Accept", "text/event-stream")
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "", "", newLastID, ctx.Err()
		}
		return "reconnect", "", newLastID, nil
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 401:
		return "", "", newLastID, authErr("Unauthorized — check your API key")
	case 429:
		return "", "", newLastID, &RateLimitError{baseError: baseError{
			Message:    "Rate limit exceeded on SSE endpoint",
			StatusCode: 429,
			RequestID:  requestID(resp.Header),
		}}
	case 200:
	default:
		// A status, not a dropped connection: the endpoint answered and said
		// no. Budgeted separately — see sseMaxStatusRefusals.
		return "refused", "", newLastID, nil
	}

	scanner := bufio.NewScanner(resp.Body)
	// Allow large SSE frames
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	event := newSSEEvent()
	flush := func() (string, string, error) {
		dataRaw, hasData := event.data()
		if !hasData {
			event = newSSEEvent()
			return "", "", nil
		}
		eventType := event.fields["event"]
		if eventType == "" {
			eventType = "message"
		}
		if id := event.fields["id"]; id != "" {
			newLastID = id
		}
		event = newSSEEvent()

		var data map[string]any
		if jerr := json.Unmarshal([]byte(dataRaw), &data); jerr != nil {
			data = map[string]any{"raw": dataRaw}
		}
		elapsed := time.Since(start).Seconds()

		switch eventType {
		case "progress":
			if onProgress != nil {
				onProgress(ProgressEvent{
					Completed:      asIntPtr(data["completed"]),
					Total:          asIntPtr(data["total"]),
					Step:           asString(data["step"]),
					ElapsedSeconds: elapsed,
					Raw:            data,
				})
			}
		case "completed":
			dl := asString(data["download_url"])
			if dl == "" {
				dl = fallback
			}
			return "done", dl, nil
		case "failed":
			step := asString(data["step"])
			reason := asString(data["reason"])
			if step == "" {
				step = "unknown"
			}
			if reason == "" {
				reason = "unknown"
			}
			return "", "", jobFailedErr(step, reason)
		}
		return "", "", nil
	}

	for scanner.Scan() {
		if ctx.Err() != nil {
			return "", "", newLastID, ctx.Err()
		}
		if time.Since(start) >= timeout {
			return "timeout", "", newLastID, nil
		}
		line := scanner.Text()
		if line == "" {
			outcome, url, ferr := flush()
			if ferr != nil {
				return "", "", newLastID, ferr
			}
			if outcome == "done" {
				return "done", url, newLastID, nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		event.add(line)
	}

	if ctx.Err() != nil {
		return "", "", newLastID, ctx.Err()
	}
	return "reconnect", "", newLastID, nil
}

func (c *Client) waitPoll(ctx context.Context, jobID, downloadURL string, timeout time.Duration) ([]byte, error) {
	start := time.Now()
	maxAttempts := int(timeout / pollInterval)
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if time.Since(start) >= timeout {
			break
		}
		failed, err := c.CheckFailed(ctx, []string{jobID})
		if err == nil && len(failed) > 0 && failed[0] {
			return nil, jobFailedErr("unknown", "job marked failed")
		}
		if err != nil {
			var auth *AuthenticationError
			if errors.As(err, &auth) || ctx.Err() != nil {
				return nil, err
			}
		}

		content, err := c.DownloadResult(ctx, downloadURL)
		if err == nil {
			return content, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if attempt < maxAttempts {
			if err := sleepCtx(ctx, pollInterval); err != nil {
				return nil, err
			}
		}
	}
	return nil, timeoutErr(fmt.Sprintf("Job %s did not complete within %s", jobID, timeout))
}

// sseEvent accumulates the fields of one SSE frame. Repeated data lines are
// joined with newlines, as the spec requires.
type sseEvent struct {
	fields   map[string]string
	dataLine []string
}

func newSSEEvent() *sseEvent {
	return &sseEvent{fields: map[string]string{}}
}

func (e *sseEvent) add(line string) {
	field, value, found := strings.Cut(line, ":")
	if !found {
		field, value = line, ""
	}
	value = strings.TrimPrefix(value, " ")
	if field == "data" {
		e.dataLine = append(e.dataLine, value)
		return
	}
	e.fields[field] = value
}

func (e *sseEvent) data() (string, bool) {
	if len(e.dataLine) == 0 {
		return "", false
	}
	return strings.Join(e.dataLine, "\n"), true
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// sleepCtx waits for d, or returns early if the context is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

func asIntPtr(v any) *int {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case float64:
		i := int(t)
		return &i
	case int:
		return &t
	case string:
		i, err := strconv.Atoi(t)
		if err != nil {
			return nil
		}
		return &i
	default:
		return nil
	}
}
