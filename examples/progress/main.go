// Live progress for a web app: turn the SDK's two progress callbacks into one
// 0–100 number you can store per job and serve to your frontend.
//
//	STT_API_KEY=stt_... go run ./progress
package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	stt "github.com/speechrevolutions/speechrevolutions-go"
)

// Weight the two phases into a single bar (upload is usually quick).
const uploadWeight = 0.15     // upload spans 0–15%
const transcribeWeight = 0.85 // transcription spans 15–100%

// jobProgress holds the latest overall percent for one job. Safe for concurrent
// reads (e.g. an HTTP handler) while the callbacks write.
type jobProgress struct {
	mu      sync.Mutex
	phase   string
	percent float64
}

func (p *jobProgress) set(phase string, overall float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.phase = phase
	if overall > p.percent { // never go backwards
		p.percent = overall
	}
}

func (p *jobProgress) snapshot() (string, float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.phase, p.percent
}

func main() {
	apiKey := os.Getenv("SPEECHREVOLUTIONS_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("STT_API_KEY")
	}
	client, err := stt.NewClient(apiKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	prog := &jobProgress{}

	// pct returns event.Percent() as 0..100 (0 when not yet known).
	pct := func(e stt.ProgressEvent) float64 {
		if v, ok := e.Percent(); ok {
			return v
		}
		return 0
	}

	opts := stt.TranscribeOptions{
		// Upload-phase callback (Step == "upload").
		OnUploadProgress: func(e stt.ProgressEvent) {
			prog.set("upload", pct(e)*uploadWeight)
			ph, p := prog.snapshot()
			fmt.Printf("  [%10s] %5.1f%%\n", ph, p)
		},
	}

	// Transcription-phase callback is the 3rd arg to Transcribe.
	result, err := client.Transcribe(context.Background(), "audio.mp3", opts, func(e stt.ProgressEvent) {
		prog.set("transcribe", uploadWeight*100+pct(e)*transcribeWeight)
		ph, p := prog.snapshot()
		fmt.Printf("  [%10s] %5.1f%%\n", ph, p)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	prog.set("done", 100)
	fmt.Printf("\nDone — %d chars\n", len(result.Text()))
}
