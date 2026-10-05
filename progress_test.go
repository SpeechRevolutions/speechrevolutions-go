package stt

import "testing"

func TestHumanBytes(t *testing.T) {
	cases := map[int]string{
		0:                      "0B",
		500:                    "500B",
		1023:                   "1023B",
		1024:                   "1.0KB",
		270542:                 "264.2KB",
		1980614:                "1.9MB",
		5 * 1024 * 1024:        "5.0MB",
		10845291315:            "10.1GB",
		5 * 1024 * 1024 * 1024: "5.0GB",
	}
	for n, want := range cases {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestFinalProgressAddsOnly100Once(t *testing.T) {
	ip := func(n int) *int { return &n }

	var got []ProgressEvent
	f := &finalProgress{forward: func(e ProgressEvent) { got = append(got, e) }}
	cb := f.callback()
	cb(ProgressEvent{Completed: ip(1), Total: ip(4), Step: "chunk:0"})
	f.complete()
	if len(got) != 2 || *got[1].Completed != 4 || *got[1].Total != 4 || got[1].Step != "completed" {
		t.Fatalf("partial progress should end with a 4/4 completed event, got %+v", got)
	}

	got = nil
	f = &finalProgress{forward: func(e ProgressEvent) { got = append(got, e) }}
	f.callback()(ProgressEvent{Completed: ip(4), Total: ip(4)})
	f.complete()
	if len(got) != 1 {
		t.Fatalf("a stream that already reached 100%% must not get a second one, got %d events", len(got))
	}

	(&finalProgress{}).complete() // no callback: must not panic
}

func TestToDeepgramSpeakersAreZeroBased(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	words := []Word{
		{Word: "Hi.", Start: f(0), End: f(0.5), Speaker: "SPEAKER_1"},
		{Word: "Hello", Start: f(0.6), End: f(1), Speaker: "SPEAKER_2"},
	}
	tr := Transcript{JobID: "j", Words: words, Utterances: utterancesFromWords(words)}
	res := tr.ToDeepgram()["results"].(map[string]any)
	dw := res["channels"].([]map[string]any)[0]["alternatives"].([]map[string]any)[0]["words"].([]map[string]any)
	if dw[0]["speaker"] != 0 || dw[1]["speaker"] != 1 {
		t.Errorf("word speakers = %v, %v; want 0, 1", dw[0]["speaker"], dw[1]["speaker"])
	}
	if dw[0]["word"] != "hi" || dw[0]["punctuated_word"] != "Hi." {
		t.Errorf("word shape = %v", dw[0])
	}
	utts := res["utterances"].([]map[string]any)
	if len(utts) != 2 || utts[0]["speaker"] != 0 || utts[1]["speaker"] != 1 || utts[1]["transcript"] != "Hello" {
		t.Errorf("utterances = %v", utts)
	}
	if deepgramSpeaker("A") != "A" {
		t.Error("an unrecognised label must pass through unchanged")
	}
}
