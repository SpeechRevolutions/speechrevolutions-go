package stt

// Live tests for 1.1 against the REAL API: add-on options, DeleteJob and real-time streaming,
// including a cut connection that must resume.
//
//	SR_LIVE=1 SPEECHREVOLUTIONS_API_KEY=stt_... go test -run LiveFeature -v

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	liveCall   = "testdata/call.mp3"   // bank call: names, card, phone, "Northwind Bank"
	liveNoName = "testdata/noname.mp3" // two speakers, quarterly numbers, no names said
	liveProf   = "testdata/prof.mp3"   // three strong swear words
)

func norm(s string) string {
	s = strings.NewReplacer(".", " ", ",", " ").Replace(strings.ToLower(s))
	return strings.Join(strings.Fields(s), " ")
}

func featureCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestLiveFeatureSpeakerLabelsDefaultOff(t *testing.T) {
	c := liveAPIClient(t)
	tr, err := c.Transcribe(featureCtx(t), liveNoName, TranscribeOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(norm(tr.Text()), "quarterly") {
		t.Fatalf("text %q", tr.Text())
	}
	for _, w := range tr.Words {
		if w.Speaker != "" {
			t.Fatalf("speaker %q present with speaker labels off", w.Speaker)
		}
	}
	tr, err = c.Transcribe(featureCtx(t), liveNoName, TranscribeOptions{SpeakerLabels: Bool(true)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	spk := map[string]bool{}
	for _, w := range tr.Words {
		spk[w.Speaker] = true
	}
	if len(spk) != 2 {
		t.Fatalf("speakers %v", spk)
	}
}

func TestLiveFeatureSummaryParagraphs(t *testing.T) {
	c := liveAPIClient(t)
	tr, err := c.Transcribe(featureCtx(t), liveNoName, TranscribeOptions{SpeakerLabels: Bool(true), Summarize: true, Paragraphs: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tr.Summary == nil || len(tr.Summary.Text) < 40 || len(tr.Summary.Bullets) == 0 {
		t.Fatalf("summary %+v", tr.Summary)
	}
	if len(tr.Paragraphs) < 2 || len(tr.Paragraphs[0].Sentences) == 0 {
		t.Fatalf("paragraphs %+v", tr.Paragraphs)
	}
}

func TestLiveFeatureProfanityAndExtra(t *testing.T) {
	c := liveAPIClient(t)
	tr, err := c.Transcribe(featureCtx(t), liveProf, TranscribeOptions{FilterProfanity: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(tr.Text(), "s***") || strings.Contains(strings.ToLower(tr.Text()), "shit") {
		t.Fatalf("text %q", tr.Text())
	}
	tr, err = c.Transcribe(featureCtx(t), liveProf, TranscribeOptions{Extra: map[string]any{"filter_profanity": true}}, nil)
	if err != nil || !strings.Contains(tr.Text(), "s***") {
		t.Fatalf("extra: %v %q", err, tr.Text())
	}
}

func TestLiveFeatureCustomVocabulary(t *testing.T) {
	c := liveAPIClient(t)
	tr, err := c.Transcribe(featureCtx(t), liveCall, TranscribeOptions{CustomVocabulary: []string{"Northwind"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(norm(tr.Text()), "northwind bank") || strings.Contains(norm(tr.Text()), "north northwind") {
		t.Fatalf("text %q", tr.Text())
	}
}

func TestLiveFeatureDeleteJob(t *testing.T) {
	c := liveAPIClient(t)
	ctx := featureCtx(t)
	id, err := c.Submit(ctx, liveProf, TranscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		st, err := c.GetJobStatus(ctx, id)
		if err == nil && st.Status == "completed" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err := c.DeleteJob(ctx, id); err != nil {
		if strings.Contains(err.Error(), "temporarily unavailable") {
			t.Skip("prod user_cluster lacks s3:DeleteObject (2026-11-23)")
		}
		t.Fatal(err)
	}
	if _, err := c.GetTranscript(ctx, id, OutputJSON); err == nil {
		t.Fatal("transcript still readable after DeleteJob")
	}
}

func TestLiveFeatureStreamFile(t *testing.T) {
	c := liveAPIClient(t)
	var mu sync.Mutex
	seen := map[string]bool{}
	var detected string
	res, err := c.StreamFile(featureCtx(t), liveNoName, StreamOptions{OnEvent: func(e StreamEvent) {
		mu.Lock()
		seen[e.Type] = true
		if e.Type == "language.detected" && detected == "" {
			detected = e.Language
		}
		mu.Unlock()
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Complete || res.JobID == "" || !strings.Contains(norm(res.Text), "planning meeting") {
		t.Fatalf("result %+v", res)
	}
	for _, ty := range []string{"session.created", "transcription.partial", "final", "transcription.done"} {
		if !seen[ty] {
			t.Errorf("no %s event", ty)
		}
	}
	if detected != "en" {
		t.Errorf("detected %q", detected)
	}
}

func TestLiveFeatureStreamTurnsAndVocabulary(t *testing.T) {
	c := liveAPIClient(t)
	var mu sync.Mutex
	seen := map[string]bool{}
	t0 := time.Now()
	res, err := c.StreamFile(featureCtx(t), liveCall, StreamOptions{Language: "en", Vocabulary: []string{"Northwind"},
		OnEvent: func(e StreamEvent) {
			mu.Lock()
			seen[e.Type] = true
			mu.Unlock()
			if e.Type != "transcription.partial" {
				t.Logf("%6.2fs %s %s", time.Since(t0).Seconds(), e.Type, e.Code)
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(norm(res.Text), "northwind bank") {
		t.Fatalf("text %q", res.Text)
	}
	if !seen["speech.started"] || !seen["turn.ended"] {
		t.Fatalf("events %v", seen)
	}
}

func TestLiveFeatureStreamResumesAfterCut(t *testing.T) {
	c := liveAPIClient(t)
	ctx := featureCtx(t)
	var mu sync.Mutex
	resumed := false
	s, err := c.Stream(ctx, StreamOptions{Language: "en", OnEvent: func(e StreamEvent) {
		if e.Type == "session.resumed" {
			mu.Lock()
			resumed = true
			mu.Unlock()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	jobID := s.JobID
	pcm, err := LoadPCM16(liveNoName)
	if err != nil {
		t.Fatal(err)
	}
	step := chunkMs * bytesPerMs
	half := len(pcm) / 2
	t0 := time.Now()
	for i, n := 0, 0; i < len(pcm); i, n = i+step, n+1 {
		end := i + step
		if end > len(pcm) {
			end = len(pcm)
		}
		_ = s.SendAudio(pcm[i:end])
		if i <= half && half < end {
			s.simulateDrop()
		}
		time.Sleep(time.Until(t0.Add(time.Duration(float64((n+1)*chunkMs)/1.3) * time.Millisecond)))
	}
	res, err := s.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !resumed {
		t.Fatal("expected a session.resumed")
	}
	if !res.Complete || res.JobID != jobID {
		t.Fatalf("result %+v (job %s)", res, jobID)
	}
	if !strings.Contains(norm(res.Text), "quarterly") || !strings.Contains(norm(res.Text), "deck with the numbers") {
		t.Fatalf("text %q", res.Text)
	}
}

func TestLiveFeatureStreamBadKey(t *testing.T) {
	_ = liveAPIClient(t)
	c, _ := NewClient("stt_" + strings.Repeat("0", 64))
	if _, err := c.Stream(context.Background(), StreamOptions{Language: "en"}); err == nil {
		t.Fatal("expected a refusal")
	}
}
