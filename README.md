# go-sdk

Official Go client for the Speech Revolutions speech-to-text API.

## Install

```bash
go get github.com/speechrevolutions/go-sdk
```

```go
import stt "github.com/speechrevolutions/go-sdk"
```

## Quick start

```go
client, _ := stt.NewClient("") // SPEECHREVOLUTIONS_API_KEY or STT_API_KEY

sl := true
result, err := client.Transcribe("meeting.mp3", stt.TranscribeOptions{
    SpeakerLabels: &sl,
}, nil)
if err != nil {
    log.Fatal(err)
}
fmt.Println(result.Text())

for _, u := range result.Utterances {
    fmt.Printf("Speaker %s: %s\n", u.Speaker, u.Text)
}
```

`Transcribe` accepts a local file path, an `http(s)` URL, or raw bytes
(`TranscribeBytes`). The third argument is an optional transcription-progress
callback (`nil` for none).

### From a URL (Deepgram-style)

```go
result, err := client.TranscribeURL("https://example.com/audio.mp3", stt.TranscribeOptions{}, nil)
// or, since Transcribe detects http(s):
result, err := client.Transcribe("https://example.com/audio.mp3", stt.TranscribeOptions{}, nil)
```

`TranscribeFile` is the same for a local path.

## Options

`TranscribeOptions` fields (bool/tier fields are pointers so unset ≠ false —
leave them `nil` to accept the default):

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `OutputType` | `OutputType` | `OutputJSON` | `txt` \| `json` \| `srt` \| `vtt` \| `docx` \| `pdf` |
| `WordTimestamps` | `*bool` | `true` | per-word start/end times |
| `SpeakerLabels` | `*bool` | `true` | label who spoke each segment |
| `Diarize` | `*bool` | — | Deepgram-compatible alias for `SpeakerLabels` |
| `NLTK` | `*bool` | `true` | restore punctuation & capitalization |
| `Tier` | `*ProcessingTier` | `TierStandard` | `standard` \| `economy` |
| `CustomVocabulary` | `[]string` | `nil` | domain terms to bias toward |
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
sl := true
result, _ := client.Transcribe("meeting.mp3", stt.TranscribeOptions{
    SpeakerLabels: &sl,
    Progress:      true,
}, nil)

// 2. Programmatic — read ProgressEvent.Percent() (0–100) to drive your own UI.
onProgress := func(e stt.ProgressEvent) { // transcription
    if pct, ok := e.Percent(); ok {
        fmt.Printf("%.0f%% %s\n", pct, e.Step) // e.g. 42 "transcribe"
    }
}
onUpload := func(e stt.ProgressEvent) { // upload (e.Step == "upload")
    if pct, ok := e.Percent(); ok {
        fmt.Printf("upload %.0f%%\n", pct)
    }
}

result, _ = client.Transcribe("meeting.mp3", stt.TranscribeOptions{
    OnUploadProgress: onUpload,
}, onProgress)
```

`ProgressEvent.Percent()` returns `(float64, bool)`; the bool is `false` when the
percentage can't be computed yet (total unknown), so treat it as "unknown".

## Webhooks & retrieving results later

`Submit` uploads and enqueues a job and returns its id **without waiting** —
ideal for batch/background work. Collect the result later via a webhook
(`CallbackURL`, a signed POST — verify `X-SR-Signature: sha256=…` against the raw
body) or by polling. See `examples/retrieve`:

```go
jobID, _ := client.Submit("meeting.mp3", stt.TranscribeOptions{}) // returns immediately
// ...or notify a webhook instead of polling:
client.Transcribe(path, stt.TranscribeOptions{CallbackURL: "https://you.example.com/hook"}, nil)

st, _ := client.GetJobStatus(jobID)          // st.Status: processing|completed|failed
if st.IsCompleted() {
    result, _ := client.GetTranscript(jobID, stt.OutputJSON) // downloads + parses
}
page, _ := client.ListJobs(50, "")           // page.Jobs, page.NextBefore
```

## Result shape

Default `OutputType` is `json`, parsed into a transcript-first object:

| Access | Like |
|--------|------|
| `result.Text()` | AssemblyAI / ElevenLabs |
| `result.TranscriptText()` | Deepgram alias |
| `result.Words` | word + start/end/speaker |
| `result.Utterances` | AssemblyAI speaker turns |
| `result.ToDeepgram()` | Deepgram-shaped map |
| `result.ToDict()` | normalized map |
| `result.Content` / `result.Save(path)` | raw bytes / write to file |

```go
dg := result.ToDeepgram()
chans := dg["results"].(map[string]any)["channels"].([]map[string]any)
fmt.Println(chans[0]["alternatives"].([]map[string]any)[0]["transcript"])

// Save writes output.<output_type> when the path has no extension.
out, _ := result.Save("output") // -> "output.json"
fmt.Println("saved to", out)
```

## Auth

```bash
export SPEECHREVOLUTIONS_API_KEY=stt_...
# or
export STT_API_KEY=stt_...
```

```go
client, _ := stt.NewClient("")          // reads the env vars above
client, _ := stt.NewClient("stt_...")   // or pass it directly
```

See [`examples/main.go`](examples/main.go) for a full run that shows progress
and saves the result. Also see [`examples/retrieve`](examples/retrieve) for
submit-and-poll, and [`examples/progress`](examples/progress) for wiring
progress into a web app.

## License

MIT — see [LICENSE](LICENSE).
