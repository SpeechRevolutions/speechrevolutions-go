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
	// false; take addresses of locals to set them explicitly.
	wordTimestamps := true
	speakerLabels := true
	nltk := true
	tier := stt.TierStandard

	result, err := client.Transcribe(
		context.Background(),
		"audio.mp3",
		stt.TranscribeOptions{
			OutputType:       stt.OutputJSON,
			WordTimestamps:   &wordTimestamps,
			SpeakerLabels:    &speakerLabels, // alias: Diarize
			NLTK:             &nltk,
			Tier:             &tier,
			CustomVocabulary: nil,
			OnUploadProgress: nil,
			Progress:         true,
		},
		nil, // called with transcription progress
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
