package stt

import "testing"

// Utterances are whole speaker turns, in time order, and never lose a word — even when the
// service's diarization list is grouped by speaker, split at pauses, and misses a word.
func TestUtterancesAreWholeTurnsInOrderAndKeepEveryWord(t *testing.T) {
	raw := `{"words":[
	  {"word":"Why,","start":0.4,"end":0.7,"speaker":"SPEAKER_1"},
	  {"word":"my","start":0.8,"end":0.9,"speaker":"SPEAKER_1"},
	  {"word":"dear?","start":3.5,"end":3.9,"speaker":"SPEAKER_1"},
	  {"word":"She","start":5.0,"end":5.2,"speaker":"SPEAKER_2"},
	  {"word":"sighed.","start":5.3,"end":5.8,"speaker":"SPEAKER_2"},
	  {"word":"Well.","start":6.5,"end":6.9,"speaker":"SPEAKER_1"}],
	 "diarization":[
	  {"start":0.4,"end":0.9,"speaker":"SPEAKER_1"},
	  {"start":6.5,"end":6.9,"speaker":"SPEAKER_1"},
	  {"start":5.0,"end":5.8,"speaker":"SPEAKER_2"}]}`
	tr := parseTranscript("j", []byte(raw), "json", "")
	want := [][2]string{{"SPEAKER_1", "Why, my dear?"}, {"SPEAKER_2", "She sighed."}, {"SPEAKER_1", "Well."}}
	if len(tr.Utterances) != len(want) {
		t.Fatalf("got %d utterances, want %d", len(tr.Utterances), len(want))
	}
	for i, u := range tr.Utterances {
		if u.Speaker != want[i][0] || u.Text != want[i][1] {
			t.Errorf("utterance %d = %q %q, want %q %q", i, u.Speaker, u.Text, want[i][0], want[i][1])
		}
	}
}
