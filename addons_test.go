package stt

// 1.1.0 add-on options: what is sent, and what comes back.

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func payloadOf(t *testing.T, opts TranscribeOptions) map[string]any {
	t.Helper()
	b, err := json.Marshal(uploadBody(0, opts))
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]any{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestAddOnOptionsAreSentOnlyWhenSet(t *testing.T) {
	base := payloadOf(t, TranscribeOptions{})
	for _, k := range []string{"auto_chapters", "known_speakers", "redact_pii", "redact_pii_policies", "redact_pii_sub", "entity_detection"} {
		if _, ok := base[k]; ok {
			t.Fatalf("%s sent by default: %v", k, base)
		}
	}
	if base["speaker_labels"] != false { // explicit, off by default
		t.Fatalf("speaker_labels %v", base["speaker_labels"])
	}

	p := payloadOf(t, TranscribeOptions{
		AutoChapters: true, KnownSpeakers: []string{"Agent", "Customer"}, RedactPII: true,
		RedactPIIPolicies: []string{"person_name", "phone_number"}, RedactPIISub: "hash",
		EntityDetection: true, SpeakerLabels: Bool(true),
	})
	if p["auto_chapters"] != true || !reflect.DeepEqual(p["known_speakers"], []any{"Agent", "Customer"}) {
		t.Fatalf("payload %v", p)
	}
	if p["redact_pii"] != true || !reflect.DeepEqual(p["redact_pii_policies"], []any{"person_name", "phone_number"}) {
		t.Fatalf("payload %v", p)
	}
	if p["redact_pii_sub"] != "hash" || p["entity_detection"] != true || p["speaker_labels"] != true {
		t.Fatalf("payload %v", p)
	}

	// Policies and substitution need RedactPII.
	q := payloadOf(t, TranscribeOptions{RedactPIIPolicies: []string{"date"}, RedactPIISub: "hash"})
	if _, ok := q["redact_pii_policies"]; ok {
		t.Fatalf("policies sent without redact_pii: %v", q)
	}
	if _, ok := q["redact_pii_sub"]; ok {
		t.Fatalf("sub sent without redact_pii: %v", q)
	}

	found := false
	for _, n := range PIIPolicies {
		found = found || n == "person_name"
	}
	if !found || len(PIIPolicies) != 12 {
		t.Fatalf("PIIPolicies %v", PIIPolicies)
	}
}

func TestTranscriptParsesChaptersEntitiesNamesAndPIIStatus(t *testing.T) {
	raw := `{"words": [{"word": "Hi.", "start": 0.0, "end": 0.4, "speaker": "SPEAKER_1"}],
	  "chapters": [{"start": 0.0, "end": 0.4, "headline": "Greeting", "gist": "Hello."}],
	  "entities": [{"entity_type": "person_name", "text": "[PERSON_NAME]", "start": 0.0, "end": 0.4}],
	  "speaker_names": {"SPEAKER_1": "Agent"}, "pii_status": "patterns_only"}`
	tr := parseTranscript("j", []byte(raw), OutputJSON, "")
	if len(tr.Chapters) != 1 || tr.Chapters[0].Headline != "Greeting" || tr.Chapters[0].Gist != "Hello." ||
		tr.Chapters[0].End == nil || *tr.Chapters[0].End != 0.4 {
		t.Fatalf("chapters %+v", tr.Chapters)
	}
	if len(tr.Entities) != 1 || tr.Entities[0].EntityType != "person_name" || tr.Entities[0].Text != "[PERSON_NAME]" {
		t.Fatalf("entities %+v", tr.Entities)
	}
	if !reflect.DeepEqual(tr.SpeakerNames, map[string]string{"SPEAKER_1": "Agent"}) || tr.PIIStatus != "patterns_only" {
		t.Fatalf("names %v status %q", tr.SpeakerNames, tr.PIIStatus)
	}
	d := tr.ToDict()
	if d["chapters"].([]Chapter)[0].Gist != "Hello." || d["speaker_names"].(map[string]string)["SPEAKER_1"] != "Agent" {
		t.Fatalf("dict %v", d)
	}

	plain := parseTranscript("j", []byte(`{"words": []}`), OutputJSON, "")
	if plain.Chapters != nil || plain.Entities != nil || plain.SpeakerNames != nil || plain.PIIStatus != "" {
		t.Fatalf("plain %+v", plain)
	}
}

func TestStreamingOptionsReachSessionUpdate(t *testing.T) {
	m := StreamOptions{
		Language: "en", RedactPII: true, RedactPIISub: "hash", IncludePartials: Bool(true),
		Voiceprint: "vp1.x", SpeakerPolicy: "strict",
	}.sessionUpdate()
	if m["redact_pii"] != true || m["redact_pii_sub"] != "hash" || m["include_partials"] != true {
		t.Fatalf("update %v", m)
	}
	if _, ok := m["capture_voiceprint"]; ok || m["voiceprint"] != "vp1.x" || m["speaker_policy"] != "strict" {
		t.Fatalf("update %v", m)
	}
	plain := StreamOptions{RedactPIIPolicies: []string{"date"}, RedactPIISub: "hash"}.sessionUpdate()
	for _, k := range []string{"redact_pii", "redact_pii_policies", "redact_pii_sub", "include_partials", "voiceprint", "capture_voiceprint", "speaker_policy"} {
		if _, ok := plain[k]; ok {
			t.Fatalf("%s sent unset: %v", k, plain)
		}
	}
	if off := (StreamOptions{IncludePartials: Bool(false), CaptureVoiceprint: true}).sessionUpdate(); off["include_partials"] != false || off["capture_voiceprint"] != true {
		t.Fatalf("update %v", off)
	}

	ev, _ := parseStreamEvent([]byte(`{"type": "voice_focus", "active": true}`))
	if ev.Active == nil || !*ev.Active {
		t.Fatalf("voice_focus %+v", ev)
	}
	ev, _ = parseStreamEvent([]byte(`{"type": "final", "index": 0, "text": "[PERSON_NAME]", "pii_status": "patterns_only"}`))
	if ev.PIIStatus != "patterns_only" || ev.Active != nil {
		t.Fatalf("final %+v", ev)
	}
}

func TestStreamResultCarriesVoiceprint(t *testing.T) {
	f := newFakeStreamServer(t, func(n int, c *fakeConn) {
		c.send(t, map[string]any{"type": "session.created", "job_id": "job-vp"})
		c.waitFor(c.hasFinalCommit)
		c.send(t, map[string]any{"type": "final", "index": 0, "text": "hello", "start": 0, "end": 1})
		c.send(t, map[string]any{"type": "transcription.done", "text": "hello", "voiceprint": "vp1.sealed",
			"voiceprint_quality": map[string]any{"score": 0.9, "ok": true, "reasons": []any{}}})
	})
	s, err := testClient(t, f.srv.URL).Stream(context.Background(), StreamOptions{CaptureVoiceprint: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SendAudio(testPCM(400, 1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := s.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if f.conn(0).got[0]["capture_voiceprint"] != true {
		t.Fatalf("update %v", f.conn(0).got[0])
	}
	if res.Voiceprint != "vp1.sealed" || res.VoiceprintQuality["ok"] != true {
		t.Fatalf("result %+v", res)
	}
}
