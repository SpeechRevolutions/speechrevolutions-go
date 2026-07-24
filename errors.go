package stt

import "fmt"

// baseError is the base SDK error type embedded by all concrete error types.
// It must not be named "Error": a field named Error would shadow the promoted
// Error() method and stop the concrete types from satisfying the error
// interface.
type baseError struct {
	Message string
}

func (e *baseError) Error() string { return e.Message }

type AuthenticationError struct{ baseError }
type RateLimitError struct{ baseError }
type JobNotFoundError struct{ baseError }
type UploadError struct{ baseError }
type TimeoutError struct{ baseError }

type JobFailedError struct {
	baseError
	Step   string
	Reason string
}

type APIError struct {
	baseError
	StatusCode int
	Body       string
}

func authErr(msg string) error {
	return &AuthenticationError{baseError{Message: msg}}
}

func rateLimitErr(msg string) error {
	return &RateLimitError{baseError{Message: msg}}
}

func notFoundErr(msg string) error {
	return &JobNotFoundError{baseError{Message: msg}}
}

func uploadErr(msg string) error {
	return &UploadError{baseError{Message: msg}}
}

func timeoutErr(msg string) error {
	return &TimeoutError{baseError{Message: msg}}
}

func apiErr(msg string, status int, body string) error {
	return &APIError{baseError: baseError{Message: msg}, StatusCode: status, Body: body}
}

func jobFailedErr(step, reason string) error {
	return &JobFailedError{
		baseError: baseError{Message: fmt.Sprintf("Job failed at step=%s: %s", step, reason)},
		Step:      step,
		Reason:    reason,
	}
}
