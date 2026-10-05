# Speech Revolutions — Go SDK

Official Go client for the [Speech Revolutions](https://www.speechrevolutions.com) speech-to-text API.

## Install

```bash
go get github.com/speechrevolutions/speechrevolutions-go
```

```go
import stt "github.com/speechrevolutions/speechrevolutions-go"
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"

	stt "github.com/speechrevolutions/speechrevolutions-go"
)

func main() {
	client, err := stt.NewClient("") // reads SPEECHREVOLUTIONS_API_KEY
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	result, err := client.Transcribe(ctx, "meeting.mp3", stt.TranscribeOptions{
		SpeakerLabels: stt.Bool(true),
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Text())

	for _, u := range result.Utterances {
		fmt.Printf("Speaker %s: %s\n", u.Speaker, u.Text)
	}
}
```

The snippets below are bodies for that `main`: each one needs only `client` and
`ctx` (plus the imports it names).

Every call takes a `context.Context` first, so you can cancel or deadline any
request. `Transcribe` accepts a local file path, an `http(s)` URL, or raw bytes
(`TranscribeBytes`). The last argument is an optional transcription-progress
callback (`nil` for none).

### From a URL (Deepgram-style)

```go
audioURL := "https://docs.speechrevolutions.com/samples/diamond-necklace.mp3"
result, err := client.TranscribeURL(ctx, audioURL, stt.TranscribeOptions{}, nil)
// or, since Transcribe detects http(s):
// result, err := client.Transcribe(ctx, audioURL, stt.TranscribeOptions{}, nil)
if err != nil {
	log.Fatal(err)
}
fmt.Println(result.Text())
```

The platform fetches the URL itself — the audio never passes through this
process.

`TranscribeFile` is the same for a local path.

## Options

`TranscribeOptions` fields (bool/tier fields are pointers so unset ≠ false —
leave them `nil` to accept the default, or set them with `stt.Bool(true)` /
`stt.Tier(stt.TierStandard)`):

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `OutputType` | `OutputType` | `OutputJSON` | `txt` \| `json` \| `srt` \| `vtt` \| `docx` \| `pdf` |
| `WordTimestamps` | `*bool` | `true` | per-word start/end times |
| `SpeakerLabels` | `*bool` | `true` | label who spoke each segment |
| `Diarize` | `*bool` | — | Deepgram-compatible alias for `SpeakerLabels` |
| `NLTK` | `*bool` | `true` | restore punctuation & capitalization |
| `Tier` | `*ProcessingTier` | `TierStandard` | `standard` — the only tier currently available |
| `CustomVocabulary` | `[]string` | `nil` | domain terms to bias toward |
| `Language` | `string` | `""` (auto-detect) | ISO 639-1 code (e.g. `"en"`, `"ru"`) to skip language detection; a wrong code makes the model translate into it, an unsupported one returns HTTP 422 |
| `OnUploadProgress` | `ProgressFunc` | `nil` | upload byte-progress callback |
| `Progress` | `bool` | `false` | render live console bars |

An empty `TranscribeOptions{}` gets all defaults applied.

## Live progress

Unlike AssemblyAI/Deepgram (which give no percentage for pre-recorded audio),
you get real-time progress — for **both** the file upload and the
transcription — as a console bar, callbacks, or both. They compose: the bars
render *and* your callbacks fire for every event.

```go
// 1. Console bars — a single line on stderr, updated in place. Shows an
//    "Uploading" byte bar, then a "Transcribing" bar. Off by default.
result, err := client.Transcribe(ctx, "meeting.mp3", stt.TranscribeOptions{
	SpeakerLabels: stt.Bool(true),
	Progress:      true,
}, nil)
if err != nil {
	log.Fatal(err)
}
fmt.Println(len(result.Text()), "characters")

// 2. Programmatic — read ProgressEvent.Percent() (0–100) to drive your own UI.
onProgress := func(e stt.ProgressEvent) { // transcription
	if pct, ok := e.Percent(); ok {
		fmt.Printf("%.0f%% %s\n", pct, e.Step) // e.g. 50 "chunk:0", then 100 "completed"
	}
}
onUpload := func(e stt.ProgressEvent) { // upload (e.Step == "upload")
	if pct, ok := e.Percent(); ok {
		fmt.Printf("upload %.0f%%\n", pct)
	}
}

result, err = client.Transcribe(ctx, "meeting.mp3", stt.TranscribeOptions{
	OnUploadProgress: onUpload,
}, onProgress)
if err != nil {
	log.Fatal(err)
}
fmt.Println(result.Text())
```

`ProgressEvent.Percent()` returns `(float64, bool)`; the bool is `false` when the
percentage can't be computed yet (total unknown), so treat it as "unknown".

A successful transcription always ends with a 100% event (`Step == "completed"`),
even for a short file the service finishes without streaming any progress.

## Webhooks & retrieving results later

`Submit` uploads and enqueues a job and returns its id **without waiting** —
ideal for batch/background work. Collect the result later via a webhook
(`CallbackURL`, a signed POST — verify `X-SR-Signature: sha256=…` against the raw
body) or by polling. See `examples/retrieve`:

```go
jobID, err := client.Submit(ctx, "meeting.mp3", stt.TranscribeOptions{}) // returns immediately
// ...or have the finished job POSTed to your webhook instead of polling:
// jobID, err := client.Submit(ctx, "meeting.mp3", stt.TranscribeOptions{CallbackURL: "https://you.example.com/hook"})
if err != nil {
	log.Fatal(err)
}

st, err := client.GetJobStatus(ctx, jobID) // st.Status: processing|completed|failed
if err != nil {
	log.Fatal(err)
}
if st.IsCompleted() {
	result, err := client.GetTranscript(ctx, jobID, stt.OutputJSON) // downloads + parses
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Text())
}
page, err := client.ListJobs(ctx, 50, "") // page.Jobs, page.NextBefore
if err != nil {
	log.Fatal(err)
}
fmt.Println(len(page.Jobs), "recent jobs")
```

The signature is HMAC-SHA256 over the raw request body, keyed with **your account's own
webhook signing secret** — find it under API Keys in the
[console](https://console.speechrevolutions.com) and put it in
`SPEECHREVOLUTIONS_WEBHOOK_SECRET`. Compare it with a constant-time function, and verify
against the bytes you received rather than a re-serialised copy (imports: `crypto/hmac`,
`crypto/sha256`, `encoding/hex`, `io`, `net/http`, `os`):

```go
secret := []byte(os.Getenv("SPEECHREVOLUTIONS_WEBHOOK_SECRET"))
http.HandleFunc("/webhooks/stt", func(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body) // the exact bytes received
	if err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(raw)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(r.Header.Get("X-SR-Signature"))) {
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	fmt.Println("event:", string(raw)) // {"job_id": ..., "status": "completed", "download_url": ...}
	w.WriteHeader(http.StatusOK)
})
log.Fatal(http.ListenAndServe(":8000", nil))
```

Worked receivers for Python, Node, Go and C# are in the
[webhooks guide](https://docs.speechrevolutions.com/guides/webhooks).

## Result shape

Default `OutputType` is `json`, parsed into a transcript-first object:

| Access | Like |
|--------|------|
| `result.Text()` | AssemblyAI / ElevenLabs |
| `result.TranscriptText()` | Deepgram alias |
| `result.Words` | word + start/end/speaker |
| `result.Utterances` | AssemblyAI speaker turns |
| `result.ToDeepgram()` | Deepgram-shaped map (0-based integer speakers) |
| `result.ToDict()` | normalized map |
| `result.Content` / `result.Save(path)` | raw bytes / write to file |

```go
result, err := client.Transcribe(ctx, "meeting.mp3", stt.TranscribeOptions{}, nil)
if err != nil {
	log.Fatal(err)
}

dg := result.ToDeepgram()
chans := dg["results"].(map[string]any)["channels"].([]map[string]any)
fmt.Println(chans[0]["alternatives"].([]map[string]any)[0]["transcript"])

// Save writes output.<output_type> when the path has no extension.
out, err := result.Save("output") // -> "output.json"
if err != nil {
	log.Fatal(err)
}
fmt.Println("saved to", out)
```

## Timeouts and retries

Every method takes a `context.Context`, so cancelling or deadlining a call is
the caller's choice:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()
result, err := client.Transcribe(ctx, "meeting.mp3", stt.TranscribeOptions{}, nil)
if err != nil {
	log.Fatal(err) // context.DeadlineExceeded after 5 minutes
}
fmt.Println(result.Text())
```

JSON API requests that fail to connect or return 429/500/502/503/504 are retried
with exponential backoff, honoring `Retry-After`. Uploads and the progress
stream have their own retry loops.

```go
client.Timeout = 10 * time.Minute // whole-job wait (SSE + polling)
client.MaxRetries = 3             // extra attempts per API request
client.RetryBackoff = 500 * time.Millisecond
// proxies, tracing, etc.: any *http.Client
client.HTTP = &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment}}
```

## Auth

```bash
export SPEECHREVOLUTIONS_API_KEY=stt_...
```

```go
client, err := stt.NewClient("") // reads SPEECHREVOLUTIONS_API_KEY
// client, err := stt.NewClient("stt_...") // or pass the key directly
if err != nil {
	log.Fatal(err) // no key set
}
```

A key the API rejects surfaces as `*stt.AuthenticationError` on the first call.

See [`examples/main.go`](examples/main.go) for a full run that shows progress
and saves the result. Also see [`examples/retrieve`](examples/retrieve) for
submit-and-poll, and [`examples/progress`](examples/progress) for wiring
progress into a web app.

## Links

- [Speech Revolutions](https://www.speechrevolutions.com) — the speech-to-text API this library talks to
- [Documentation](https://docs.speechrevolutions.com) — API reference, guides and quickstarts

## License

MIT — see [LICENSE](LICENSE).
