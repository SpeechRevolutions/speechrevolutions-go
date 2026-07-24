package stt

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
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
	pollInterval      = 5 * time.Second
)

// Client talks to the Speech Revolutions STT API.
type Client struct {
	APIKey  string
	BaseURL string
	Timeout time.Duration
	HTTP    *http.Client
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
		APIKey:  apiKey,
		BaseURL: defaultBaseURL,
		Timeout: 600 * time.Second,
		HTTP:    &http.Client{},
	}, nil
}

// Transcribe uploads audio, waits for completion, and returns a Transcript.
// audioPath may be a local filesystem path or an http(s) URL.
func (c *Client) Transcribe(audioPath string, opts TranscribeOptions, onProgress ProgressFunc) (*Transcript, error) {
	if strings.HasPrefix(audioPath, "http://") || strings.HasPrefix(audioPath, "https://") {
		return c.TranscribeURL(audioPath, opts, onProgress)
	}
	data, err := os.ReadFile(audioPath)
	if err != nil {
		return nil, err
	}
	return c.TranscribeBytes(data, opts, onProgress)
}

// TranscribeURL downloads remote audio then transcribes it.
func (c *Client) TranscribeURL(url string, opts TranscribeOptions, onProgress ProgressFunc) (*Transcript, error) {
	resp, err := c.HTTP.Get(url)
	if err != nil {
		return nil, apiErr(fmt.Sprintf("Failed to download audio URL: %v", err), 0, "")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, apiErr(fmt.Sprintf("Failed to download audio URL (HTTP %d)", resp.StatusCode), resp.StatusCode, truncate(string(data), 300))
	}
	return c.TranscribeBytes(data, opts, onProgress)
}

// TranscribeFile is an alias for Transcribe with a local path.
func (c *Client) TranscribeFile(path string, opts TranscribeOptions, onProgress ProgressFunc) (*Transcript, error) {
	return c.Transcribe(path, opts, onProgress)
}

// TranscribeBytes is like Transcribe but accepts raw audio bytes.
func (c *Client) TranscribeBytes(data []byte, opts TranscribeOptions, onProgress ProgressFunc) (*Transcript, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("audio is empty")
	}
	opts = opts.withDefaults()

	job, err := c.CreateUploadJob(len(data), opts)
	if err != nil {
		return nil, err
	}

	// Upload phase: byte-level progress -> optional callback + optional bar.
	uploadCb, uploadBar := resolveProgress(opts.OnUploadProgress, opts.Progress, "Uploading", true)
	err = c.uploadAudio(job.UploadURL, data, job.JobID, byteProgressAdapter(uploadCb))
	uploadBar.close()
	if err != nil {
		return nil, err
	}

	if err := c.CompleteUpload(job.JobID); err != nil {
		return nil, err
	}

	// Transcription phase: SSE progress -> optional callback + optional bar.
	progressCb, bar := resolveProgress(onProgress, opts.Progress, "Transcribing", false)
	content, downloadURL, err := c.WaitForResult(job.JobID, job.DownloadURL, progressCb)
	bar.close()
	if err != nil {
		return nil, err
	}
	return parseTranscript(job.JobID, content, opts.OutputType, downloadURL), nil
}

// Submit uploads and enqueues a job, returning its job_id WITHOUT waiting for
// the result. Collect it later via a webhook (opts.CallbackURL) or by polling
// GetJobStatus / GetTranscript. Ideal for batch workloads. audioPath may be a
// local path or an http(s) URL.
func (c *Client) Submit(audioPath string, opts TranscribeOptions) (string, error) {
	if strings.HasPrefix(audioPath, "http://") || strings.HasPrefix(audioPath, "https://") {
		resp, err := c.HTTP.Get(audioPath)
		if err != nil {
			return "", apiErr(fmt.Sprintf("Failed to download audio URL: %v", err), 0, "")
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		if resp.StatusCode != 200 {
			return "", apiErr(fmt.Sprintf("Failed to download audio URL (HTTP %d)", resp.StatusCode), resp.StatusCode, truncate(string(data), 300))
		}
		return c.SubmitBytes(data, opts)
	}
	data, err := os.ReadFile(audioPath)
	if err != nil {
		return "", err
	}
	return c.SubmitBytes(data, opts)
}

// SubmitBytes is like Submit but accepts raw audio bytes.
func (c *Client) SubmitBytes(data []byte, opts TranscribeOptions) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("audio is empty")
	}
	opts = opts.withDefaults()

	job, err := c.CreateUploadJob(len(data), opts)
	if err != nil {
		return "", err
	}
	uploadCb, uploadBar := resolveProgress(opts.OnUploadProgress, opts.Progress, "Uploading", true)
	err = c.uploadAudio(job.UploadURL, data, job.JobID, byteProgressAdapter(uploadCb))
	uploadBar.close()
	if err != nil {
		return "", err
	}
	if err := c.CompleteUpload(job.JobID); err != nil {
		return "", err
	}
	return job.JobID, nil
}

type uploadRequest struct {
	FileSize         int      `json:"file_size"`
	OutputType       string   `json:"output_type"`
	WordTimestamps   bool     `json:"word_timestamps"`
	SpeakerLabels    bool     `json:"speaker_labels"`
	NLTK             bool     `json:"nltk"`
	Tier             string   `json:"tier"`
	CustomVocabulary []string `json:"custom_vocabulary,omitempty"`
	CallbackURL      string   `json:"callback_url,omitempty"`
}

type uploadResponse struct {
	JobID       string          `json:"job_id"`
	UploadURL   json.RawMessage `json:"upload_url"`
	DownloadURL string          `json:"download_url"`
	ContentType string          `json:"content_type"`
	ExpiresIn   int             `json:"expires_in"`
}

// CreateUploadJob calls POST /api/v1/upload.
func (c *Client) CreateUploadJob(fileSize int, opts TranscribeOptions) (*UploadJob, error) {
	opts = opts.withDefaults()
	body := uploadRequest{
		FileSize:         fileSize,
		OutputType:       string(opts.OutputType),
		WordTimestamps:   *opts.WordTimestamps,
		SpeakerLabels:    *opts.SpeakerLabels,
		NLTK:             *opts.NLTK,
		Tier:             string(*opts.Tier),
		CustomVocabulary: opts.CustomVocabulary,
		CallbackURL:      opts.CallbackURL,
	}
	var resp uploadResponse
	if err := c.apiRequest("POST", "/api/v1/upload", body, &resp); err != nil {
		return nil, err
	}

	var uploadURL any
	var asString string
	if err := json.Unmarshal(resp.UploadURL, &asString); err == nil {
		uploadURL = asString
	} else {
		var post PresignedPost
		if err := json.Unmarshal(resp.UploadURL, &post); err != nil {
			return nil, apiErr("invalid upload_url in response", 0, string(resp.UploadURL))
		}
		uploadURL = post
	}

	return &UploadJob{
		JobID:       resp.JobID,
		UploadURL:   uploadURL,
		DownloadURL: resp.DownloadURL,
		ContentType: resp.ContentType,
		ExpiresIn:   resp.ExpiresIn,
	}, nil
}

// TouchUploadProgress calls POST /api/v1/upload/progress.
func (c *Client) TouchUploadProgress(jobID string) error {
	return c.apiRequest("POST", "/api/v1/upload/progress", map[string]string{"job_id": jobID}, nil)
}

// UploadAudio streams bytes to the presigned URL, with progress heartbeats.
func (c *Client) UploadAudio(uploadURL any, data []byte, jobID string) error {
	return c.uploadAudio(uploadURL, data, jobID, nil)
}

// uploadAudio is UploadAudio with an optional byte-level progress callback
// (sent, total) reported as bytes are streamed to the presigned target.
func (c *Client) uploadAudio(uploadURL any, data []byte, jobID string, byteCb func(sent, total int)) error {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	if jobID != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(uploadProgressInterval)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					_ = c.TouchUploadProgress(jobID)
				}
			}
		}()
	}

	var lastErr error
	for attempt := 1; attempt <= uploadMaxAttempts; attempt++ {
		lastErr = c.putOrPostUpload(uploadURL, data, byteCb)
		if lastErr == nil {
			close(stop)
			wg.Wait()
			return nil
		}
		if attempt < uploadMaxAttempts {
			time.Sleep(uploadBaseDelay * time.Duration(1<<(attempt-1)))
		}
	}
	close(stop)
	wg.Wait()
	return uploadErr(fmt.Sprintf("Upload failed after %d attempts: %v", uploadMaxAttempts, lastErr))
}

// CompleteUpload calls POST /api/v1/upload/complete.
func (c *Client) CompleteUpload(jobID string) error {
	return c.apiRequest("POST", "/api/v1/upload/complete", map[string]string{"job_id": jobID}, nil)
}

// WaitForResult waits via SSE (with polling fallback) and downloads the result.
func (c *Client) WaitForResult(jobID, downloadURL string, onProgress ProgressFunc) ([]byte, string, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 600 * time.Second
	}

	url, err := c.waitSSE(jobID, downloadURL, onProgress, timeout)
	if err != nil {
		return nil, "", err
	}
	if url == "" {
		content, err := c.waitPoll(jobID, downloadURL, timeout)
		return content, downloadURL, err
	}
	content, err := c.DownloadResult(url)
	return content, url, err
}

// DownloadResult GETs the result bytes from a download URL.
func (c *Client) DownloadResult(downloadURL string) ([]byte, error) {
	resp, err := c.HTTP.Get(downloadURL)
	if err != nil {
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
func (c *Client) CancelJob(jobID string) error {
	return c.apiRequest("POST", "/api/v1/jobs/cancel", map[string]string{"job_id": jobID}, nil)
}

// CheckFailed calls POST /api/v1/jobs/check-failed.
func (c *Client) CheckFailed(jobIDs []string) ([]bool, error) {
	var resp struct {
		FailedJobs []bool `json:"failed_jobs"`
	}
	if err := c.apiRequest("POST", "/api/v1/jobs/check-failed", map[string]any{"job_ids": jobIDs}, &resp); err != nil {
		return nil, err
	}
	return resp.FailedJobs, nil
}

// GetJobStatus calls GET /api/v1/jobs/{id} for the current status (and a fresh
// download URL once the job has completed).
func (c *Client) GetJobStatus(jobID string) (*JobStatus, error) {
	var st JobStatus
	if err := c.apiRequest("GET", "/api/v1/jobs/"+url.PathEscape(jobID), nil, &st); err != nil {
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
func (c *Client) GetTranscript(jobID string, outputType OutputType) (*Transcript, error) {
	if outputType == "" {
		outputType = OutputJSON
	}
	st, err := c.GetJobStatus(jobID)
	if err != nil {
		return nil, err
	}
	if st.IsFailed() {
		return nil, jobFailedErr(st.FailedStage, st.Reason)
	}
	if !st.IsCompleted() || st.DownloadURL == "" {
		return nil, fmt.Errorf("job %s is not complete (status=%s)", jobID, st.Status)
	}
	content, err := c.DownloadResult(st.DownloadURL)
	if err != nil {
		return nil, err
	}
	return parseTranscript(jobID, content, outputType, st.DownloadURL), nil
}

// ListJobs calls GET /api/v1/jobs — the caller's most-recent jobs, newest
// first, cursor-paginated. Pass the returned NextBefore as before for the next
// page; limit <= 0 defaults to 50.
func (c *Client) ListJobs(limit int, before string) (*JobList, error) {
	if limit <= 0 {
		limit = 50
	}
	path := "/api/v1/jobs?limit=" + strconv.Itoa(limit)
	if before != "" {
		path += "&before=" + url.QueryEscape(before)
	}
	var out JobList
	if err := c.apiRequest("GET", path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) apiRequest(method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, strings.TrimRight(c.BaseURL, "/")+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return apiErr(fmt.Sprintf("Cannot connect to %s: %v", c.BaseURL, err), 0, "")
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if err := c.raiseForStatus(resp.StatusCode, respBody); err != nil {
		return err
	}
	if out == nil || len(respBody) == 0 {
		return nil
	}
	return json.Unmarshal(respBody, out)
}

func (c *Client) raiseForStatus(status int, body []byte) error {
	if status == 200 || status == 204 {
		return nil
	}
	text := truncate(string(body), 300)
	switch status {
	case 401:
		return authErr("Unauthorized — check your API key")
	case 404:
		return notFoundErr("Job not found or upload session expired")
	case 429:
		return rateLimitErr("Rate limit exceeded — try again shortly")
	default:
		return apiErr(fmt.Sprintf("Unexpected response (HTTP %d)", status), status, text)
	}
}

func (c *Client) putOrPostUpload(uploadURL any, data []byte, byteCb func(sent, total int)) error {
	switch u := uploadURL.(type) {
	case string:
		// Stream the body through a counting reader while keeping Content-Length
		// correct and explicit — a presigned S3 PUT rejects chunked transfer
		// encoding, so ContentLength must be set so net/http does NOT chunk.
		var body io.Reader = bytes.NewReader(data)
		if byteCb != nil {
			body = &progressReader{data: data, cb: byteCb}
		}
		req, err := http.NewRequest("PUT", u, body)
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

	case PresignedPost:
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		for k, v := range u.Fields {
			_ = w.WriteField(k, v)
		}
		h := make(textproto.MIMEHeader)
		h.Set("Content-Disposition", `form-data; name="file"; filename="audio"`)
		h.Set("Content-Type", "application/octet-stream")
		part, err := w.CreatePart(h)
		if err != nil {
			return err
		}
		if _, err := part.Write(data); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		payload := buf.Bytes()
		var body io.Reader = bytes.NewReader(payload)
		if byteCb != nil {
			body = &progressReader{data: payload, cb: byteCb}
		}
		req, err := http.NewRequest("POST", u.URL, body)
		if err != nil {
			return err
		}
		req.ContentLength = int64(len(payload))
		req.Header.Set("Content-Type", w.FormDataContentType())
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

	default:
		return uploadErr(fmt.Sprintf("unsupported upload_url type: %T", uploadURL))
	}
}

func (c *Client) waitSSE(jobID, fallback string, onProgress ProgressFunc, timeout time.Duration) (string, error) {
	start := time.Now()
	var lastEventID string
	reconnects := 0

	for {
		if time.Since(start) >= timeout {
			return "", timeoutErr(fmt.Sprintf("Timed out after %s waiting for job %s", timeout, jobID))
		}
		if reconnects > sseMaxReconnects {
			return "", nil // signal polling fallback
		}
		if reconnects > 0 {
			time.Sleep(sseReconnectDelay)
		}

		outcome, url, newID, err := c.sseAttempt(jobID, start, timeout, lastEventID, fallback, onProgress)
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
		case "reconnect":
			reconnects++
		}
	}
}

func (c *Client) sseAttempt(
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

	req, err := http.NewRequest("GET", strings.TrimRight(c.BaseURL, "/")+"/api/v1/jobs/"+jobID+"/stream", nil)
	if err != nil {
		return "reconnect", "", newLastID, nil
	}
	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Accept", "text/event-stream")
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	remaining := timeout - time.Since(start)
	client := &http.Client{Timeout: remaining}
	resp, err := client.Do(req)
	if err != nil {
		return "reconnect", "", newLastID, nil
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 401:
		return "", "", newLastID, authErr("Unauthorized — check your API key")
	case 429:
		return "", "", newLastID, rateLimitErr("Rate limit exceeded on SSE endpoint")
	case 200:
	default:
		return "reconnect", "", newLastID, nil
	}

	scanner := bufio.NewScanner(resp.Body)
	// Allow large SSE frames
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	event := map[string]string{}
	flush := func() (string, string, error) {
		dataRaw := event["data"]
		if dataRaw == "" {
			event = map[string]string{}
			return "", "", nil
		}
		eventType := event["event"]
		if eventType == "" {
			eventType = "message"
		}
		if id := event["id"]; id != "" {
			newLastID = id
		}

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
			event = map[string]string{}
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
		event = map[string]string{}
		return "", "", nil
	}

	for scanner.Scan() {
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
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		event[field] = value
	}

	return "reconnect", "", newLastID, nil
}

func (c *Client) waitPoll(jobID, downloadURL string, timeout time.Duration) ([]byte, error) {
	start := time.Now()
	maxAttempts := int(timeout / pollInterval)
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if time.Since(start) >= timeout {
			break
		}
		failed, err := c.CheckFailed([]string{jobID})
		if err == nil && len(failed) > 0 && failed[0] {
			return nil, jobFailedErr("unknown", "job marked failed")
		}
		if _, ok := err.(*AuthenticationError); ok {
			return nil, err
		}

		resp, err := c.HTTP.Get(downloadURL)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return body, nil
			}
		}

		if attempt < maxAttempts {
			time.Sleep(pollInterval)
		}
	}
	return nil, timeoutErr(fmt.Sprintf("Job %s did not complete within %s", jobID, timeout))
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
