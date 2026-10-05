// Minimal example — shows live progress, saves the result to disk.
//
// Every option is spelled out explicitly (no reliance on defaults) so you can
// see every knob Transcribe exposes.
package main

import (
	"context"
	"fmt"
	"os"

	stt "github.com/speechrevolutions/speechrevolutions-go"
)

func main() {
	// Reads the key from SPEECHREVOLUTIONS_API_KEY.
	apiKey := os.Getenv("SPEECHREVOLUTIONS_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "Set SPEECHREVOLUTIONS_API_KEY.")
		os.Exit(1)
	}

	client, err := stt.NewClient(apiKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// *bool / *ProcessingTier options are pointers so "unset" is distinct from
	// false; stt.Bool and stt.Tier build them inline.

	result, err := client.Transcribe(
		context.Background(),
		"audio.mp3",
		stt.TranscribeOptions{
			OutputType:       stt.OutputJSON,
			WordTimestamps:   stt.Bool(true),
			SpeakerLabels:    stt.Bool(true), // alias: Diarize
			NLTK:             stt.Bool(true),
			Tier:             stt.Tier(stt.TierStandard), // the only tier available today
			CustomVocabulary: nil,
			Language:         "", // "" = auto-detect; e.g. "en" to pin English
			OnUploadProgress: nil,
			Progress:         true,
		},
		nil, // or a func(stt.ProgressEvent) for transcription progress
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	out, err := result.Save("output") // writes output.<output_type>; returns the path
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Saved transcript to %s\n", out)
}
