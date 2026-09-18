package stt

import (
	"encoding/json"
	"os"
	"strings"
)

// OutputType is the transcription result format.
type OutputType string

const (
	OutputTXT  OutputType = "txt"
	OutputJSON OutputType = "json"
	OutputSRT  OutputType = "srt"
	OutputVTT  OutputType = "vtt"
	OutputDOCX OutputType = "docx"
	OutputPDF  OutputType = "pdf"
)

// ProcessingTier selects pricing / scheduling.
type ProcessingTier string

const (
	TierStandard ProcessingTier = "standard"
	TierEconomy  ProcessingTier = "economy"
)

// TranscribeOptions controls how audio is transcribed.
// Bool returns a pointer to b, for the optional *bool fields on
// TranscribeOptions. Without it every call site needs a throwaway variable:
//
//	opts := stt.TranscribeOptions{SpeakerLabels: stt.Bool(true)}
func Bool(b bool) *bool { return &b }

// Tier returns a pointer to t, for TranscribeOptions.Tier.
func Tier(t ProcessingTier) *ProcessingTier { return &t }

type TranscribeOptions struct {
	OutputType       OutputType
	WordTimestamps   *bool
	SpeakerLabels    *bool
	Diarize          *bool // alias for SpeakerLabels
	NLTK             *bool
	Tier             *ProcessingTier
	CustomVocabulary []string
	// CallbackURL, if set, is an http(s) webhook POSTed a signed
	// completion/failure notification (X-SR-Signature: sha256=...).
	CallbackURL string

	// OnUploadProgress, if set, is called with a ProgressEvent (Step == "upload")
	// for every byte-level upload update. Read Percent() for a 0–100 value.
	OnUploadProgress ProgressFunc
	// Progress, when true, renders live single-line progress bars to os.Stderr:
	// an "Uploading" byte bar, then a "Transcribing" bar. Off by default. It
	// composes with the callbacks — the bars render and your callbacks still fire.
	Progress bool
}

func (o TranscribeOptions) withDefaults() TranscribeOptions {
	out := o
	if out.OutputType == "" {
		out.OutputType = OutputJSON
	}
	t := true
	if out.WordTimestamps == nil {
		out.WordTimestamps = &t
	}
	if out.Diarize != nil {
		out.SpeakerLabels = out.Diarize
	}
	if out.SpeakerLabels == nil {
		out.SpeakerLabels = &t
	}
	if out.NLTK == nil {
		out.NLTK = &t
	}
	if out.Tier == nil {
		std := TierStandard
		out.Tier = &std
	}
	return out
}

// UploadJob is the result of POST /api/v1/upload.
type UploadJob struct {
	JobID       string
	UploadURL   string
	DownloadURL string
	ContentType string
	ExpiresIn   int
}

// ProgressEvent is a single progress update.
type ProgressEvent struct {
	Completed      *int
	Total          *int
	Step           string
	ElapsedSeconds float64
	Raw            map[string]any
}

// Percent returns completion as a 0–100 value, clamped to [0,100]. The second
// return value is false when it can't be computed yet (Completed or Total is
// nil, or Total is 0) — treat that as "unknown".
func (e ProgressEvent) Percent() (float64, bool) {
	if e.Completed == nil || e.Total == nil || *e.Total == 0 {
		return 0, false
	}
	pct := float64(*e.Completed) / float64(*e.Total) * 100.0
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return pct, true
}

// ProgressFunc is called on each progress event.
type ProgressFunc func(ProgressEvent)

// Word is a single transcribed word.
type Word struct {
	Word       string   `json:"word"`
	Start      *float64 `json:"start,omitempty"`
	End        *float64 `json:"end,omitempty"`
	Speaker    string   `json:"speaker,omitempty"`
	Confidence *float64 `json:"confidence,omitempty"`
	Language   string   `json:"language,omitempty"`
}

// Text is an AssemblyAI-compatible alias.
func (w Word) Text() string { return w.Word }

// LanguageSegment is a contiguous time range spoken in a single detected language.
type LanguageSegment struct {
	Start    float64 `json:"start"`
	End      float64 `json:"end"`
	Language string  `json:"language"`
}

// Utterance is a contiguous speaker turn.
type Utterance struct {
	Text    string   `json:"text"`
	Speaker string   `json:"speaker,omitempty"`
	Start   *float64 `json:"start,omitempty"`
	End     *float64 `json:"end,omitempty"`
	Words   []Word   `json:"words"`
}

// Transcript is the high-level transcription result.
type Transcript struct {
	JobID       string
	Content     []byte
	DownloadURL string
	OutputType  OutputType
	Words       []Word
	Utterances  []Utterance
	Languages   []LanguageSegment
	Raw         map[string]any
	text        string
}

// Text returns the full transcript (AssemblyAI / ElevenLabs-style).
func (t Transcript) Text() string {
	if t.text != "" {
		return t.text
	}
	return joinWords(t.Words)
}

// TranscriptText is a Deepgram-compatible alias for Text.
func (t Transcript) TranscriptText() string { return t.Text() }

// Save writes the raw result bytes to disk and returns the path written. If
// path has no file extension, "."+OutputType is appended (e.g. "output" ->
// "output.json"). The client never writes files on its own — call this.
func (t Transcript) Save(path string) (string, error) {
	base := path
	if i := strings.LastIndex(path, "/"); i >= 0 {
		base = path[i+1:]
	}
	out := path
	if !strings.Contains(base, ".") {
		out = path + "." + string(t.OutputType)
	}
	if err := os.WriteFile(out, t.Content, 0o644); err != nil {
		return "", err
	}
	return out, nil
}

// ToDict returns an AssemblyAI-inspired normalized map.
func (t Transcript) ToDict() map[string]any {
	d := map[string]any{
		"id":          t.JobID,
		"status":      "completed",
		"text":        t.Text(),
		"words":       t.Words,
		"utterances":  t.Utterances,
		"output_type": string(t.OutputType),
	}
	if len(t.Languages) > 0 {
		d["languages"] = t.Languages
	}
	return d
}

// JobStatus is the result of GET /api/v1/jobs/{id}.
type JobStatus struct {
	JobID       string `json:"job_id"`
	Status      string `json:"status"` // "processing" | "completed" | "failed"
	DownloadURL string `json:"download_url,omitempty"`
	FailedStage string `json:"failed_stage,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

// IsCompleted reports whether the job finished successfully.
func (s JobStatus) IsCompleted() bool { return s.Status == "completed" }

// IsFailed reports whether the job failed.
func (s JobStatus) IsFailed() bool { return s.Status == "failed" }

// JobSummary is one entry in a ListJobs page.
type JobSummary struct {
	JobID     string `json:"job_id"`
	CreatedAt string `json:"created_at"`
}

// JobList is a page of jobs from GET /api/v1/jobs.
type JobList struct {
	Jobs       []JobSummary `json:"jobs"`
	NextBefore string       `json:"next_before,omitempty"`
}

// ToDeepgram returns a rough Deepgram pre-recorded response shape.
func (t Transcript) ToDeepgram() map[string]any {
	dgWords := make([]map[string]any, 0, len(t.Words))
	for _, w := range t.Words {
		item := map[string]any{
			"word":            strings.TrimRight(strings.ToLower(w.Word), ".,!?;:"),
			"punctuated_word": w.Word,
		}
		if w.Start != nil {
			item["start"] = *w.Start
		}
		if w.End != nil {
			item["end"] = *w.End
		}
		if w.Speaker != "" {
			item["speaker"] = w.Speaker
		}
		dgWords = append(dgWords, item)
	}
	return map[string]any{
		"metadata": map[string]any{"request_id": t.JobID, "channels": 1},
		"results": map[string]any{
			"channels": []map[string]any{
				{"alternatives": []map[string]any{
					{"transcript": t.Text(), "confidence": 1.0, "words": dgWords},
				}},
			},
			"utterances": t.Utterances,
		},
	}
}

// TranscriptResult is kept as an alias for backwards compatibility.
type TranscriptResult = Transcript

func parseTranscript(jobID string, content []byte, outputType OutputType, downloadURL string) *Transcript {
	t := &Transcript{
		JobID:       jobID,
		Content:     content,
		DownloadURL: downloadURL,
		OutputType:  outputType,
	}
	if outputType != OutputJSON {
		t.text = string(content)
		return t
	}
	var raw map[string]any
	if err := json.Unmarshal(content, &raw); err != nil {
		t.text = string(content)
		return t
	}
	t.Raw = raw
	words := parseWords(raw["words"])
	t.Words = words
	t.Utterances = utterancesFromDiarization(words, raw["diarization"])
	t.Languages = parseLanguageSegments(raw["languages"])
	t.text = joinWords(words)
	return t
}

// utterancesFromDiarization prefers the server's diarization segments, which
// separate turns the speaker labels alone cannot (the same speaker talking
// twice). Falls back to grouping consecutive words by speaker.
func utterancesFromDiarization(words []Word, v any) []Utterance {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return utterancesFromWords(words)
	}

	out := make([]Utterance, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		start, end := asFloatPtr(m["start"]), asFloatPtr(m["end"])
		if start == nil || end == nil {
			continue
		}
		segWords := wordsWithin(words, *start, *end)
		out = append(out, Utterance{
			Text:    joinWords(segWords),
			Speaker: asString(m["speaker"]),
			Start:   start,
			End:     end,
			Words:   segWords,
		})
	}
	if len(out) == 0 {
		return utterancesFromWords(words)
	}
	return out
}

// wordsWithin collects the words a segment covers, falling back to a midpoint
// test for words that straddle the boundary.
func wordsWithin(words []Word, start, end float64) []Word {
	const eps = 1e-3
	var inside []Word
	for _, w := range words {
		if w.Start == nil || w.End == nil {
			continue
		}
		if *w.Start >= start-eps && *w.End <= end+eps {
			inside = append(inside, w)
		}
	}
	if len(inside) > 0 {
		return inside
	}
	for _, w := range words {
		if w.Start == nil || w.End == nil {
			continue
		}
		if mid := (*w.Start + *w.End) / 2; mid >= start && mid <= end {
			inside = append(inside, w)
		}
	}
	return inside
}

func parseWords(v any) []Word {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]Word, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		w := Word{Word: asString(m["word"])}
		if w.Word == "" {
			w.Word = asString(m["text"])
		}
		if s := asFloatPtr(m["start"]); s != nil {
			w.Start = s
		}
		if e := asFloatPtr(m["end"]); e != nil {
			w.End = e
		}
		w.Speaker = asString(m["speaker"])
		w.Language = asString(m["language"])
		out = append(out, w)
	}
	return out
}

func parseLanguageSegments(v any) []LanguageSegment {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]LanguageSegment, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		seg := LanguageSegment{Language: asString(m["language"])}
		if s := asFloatPtr(m["start"]); s != nil {
			seg.Start = *s
		}
		if e := asFloatPtr(m["end"]); e != nil {
			seg.End = *e
		}
		out = append(out, seg)
	}
	return out
}

func utterancesFromWords(words []Word) []Utterance {
	if len(words) == 0 {
		return nil
	}
	allNil := true
	for _, w := range words {
		if w.Speaker != "" {
			allNil = false
			break
		}
	}
	if allNil {
		return []Utterance{{Text: joinWords(words), Start: words[0].Start, End: words[len(words)-1].End, Words: words}}
	}
	var out []Utterance
	cur := []Word{words[0]}
	for _, w := range words[1:] {
		if w.Speaker == cur[0].Speaker {
			cur = append(cur, w)
		} else {
			out = append(out, utteranceFromGroup(cur))
			cur = []Word{w}
		}
	}
	out = append(out, utteranceFromGroup(cur))
	return out
}

func utteranceFromGroup(group []Word) Utterance {
	return Utterance{
		Text:    joinWords(group),
		Speaker: group[0].Speaker,
		Start:   group[0].Start,
		End:     group[len(group)-1].End,
		Words:   group,
	}
}

func joinWords(words []Word) string {
	parts := make([]string, 0, len(words))
	for _, w := range words {
		if w.Word == "" {
			continue
		}
		if len(parts) > 0 && strings.ContainsAny(w.Word[:1], ".,!?;:%)]}'\"") {
			parts[len(parts)-1] = parts[len(parts)-1] + w.Word
		} else {
			parts = append(parts, w.Word)
		}
	}
	return strings.Join(parts, " ")
}

func asFloatPtr(v any) *float64 {
	switch t := v.(type) {
	case float64:
		return &t
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return nil
		}
		return &f
	default:
		return nil
	}
}
