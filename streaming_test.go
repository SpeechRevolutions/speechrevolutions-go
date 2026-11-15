package stt

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestStreamEventMapping(t *testing.T) {
	var m map[string]any
	json.Unmarshal([]byte(`{"type":"transcription.partial","index":2,"text":"hel","stable":1,"audio_s":3.5}`), &m)
	e := toStreamEvent(m)
	if e.Type != "partial" || *e.Index != 2 || e.Text != "hel" || *e.Stable != 1 || *e.Start != 3.5 {
		t.Fatalf("partial: %+v", e)
	}
	var m2 map[string]any
	json.Unmarshal([]byte(`{"type":"error","code":"c","message":"v2 style"}`), &m2)
	if e := toStreamEvent(m2); e.Type != "error" || e.Text != "v2 style" {
		t.Fatalf("error: %+v", e)
	}
}

func TestSessionUpdate(t *testing.T) {
	th := 0.6
	got := StreamOptions{Language: "auto", Vocabulary: []string{"Acme"}, EndUserID: "u1", TurnThreshold: &th}.sessionUpdate()
	want := map[string]any{"type": "session.update", "vocabulary": []string{"Acme"}, "end_user_id": "u1",
		"turn_detection": map[string]any{"threshold": 0.6}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}
