package stt

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Response headers checked for a correlation id.
var requestIDHeaders = []string{"X-Request-Id", "X-Amzn-Requestid", "Cf-Ray"}

// baseError is the base SDK error type embedded by all concrete error types.
// It must not be named "Error": a field named Error would shadow the promoted
// Error() method and stop the concrete types from satisfying the error
// interface.
type baseError struct {
	Message string
	// StatusCode is the HTTP status that produced the error, or 0.
	StatusCode int
	// RequestID correlates the failure with server-side logs.
	RequestID string
	// Body is the truncated response body, when there was one.
	Body string
}

func (e *baseError) Error() string { return e.Message }

type AuthenticationError struct{ baseError }
type JobNotFoundError struct{ baseError }
type UploadError struct{ baseError }
type TimeoutError struct{ baseError }
type APIError struct{ baseError }

// RateLimitError carries the server's Retry-After hint in seconds, when one was
// sent in delta-seconds form.
type RateLimitError struct {
	baseError
	RetryAfter float64
}

type JobFailedError struct {
	baseError
	Step   string
	Reason string
}

func authErr(msg string) error {
	return &AuthenticationError{baseError{Message: msg}}
}

func uploadErr(msg string) error {
	return &UploadError{baseError{Message: msg}}
}

func timeoutErr(msg string) error {
	return &TimeoutError{baseError{Message: msg}}
}

func apiErr(msg string, status int, body string) error {
	return &APIError{baseError{Message: msg, StatusCode: status, Body: body}}
}

func jobFailedErr(step, reason string) error {
	return &JobFailedError{
		baseError: baseError{Message: fmt.Sprintf("Job failed at step=%s: %s", step, reason)},
		Step:      step,
		Reason:    reason,
	}
}

func requestID(h http.Header) string {
	for _, name := range requestIDHeaders {
		if v := h.Get(name); v != "" {
			return v
		}
	}
	return ""
}

// parseRetryAfter reads a Retry-After header in delta-seconds form. The
// HTTP-date form is not honored; callers fall back to their own backoff.
func parseRetryAfter(value string) (float64, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	secs, err := strconv.ParseFloat(value, 64)
	if err != nil || secs < 0 {
		return 0, false
	}
	return secs, true
}
