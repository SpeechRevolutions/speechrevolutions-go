// Submit without waiting, then retrieve later: Submit() + get-by-id + list.
//
// Submit enqueues a job and returns its id immediately (no waiting); collect the
// result later by polling GetJobStatus / GetTranscript (or via a webhook set
// through TranscribeOptions.CallbackURL).
//
//	SPEECHREVOLUTIONS_API_KEY=stt_... go run ./retrieve
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	stt "github.com/speechrevolutions/speechrevolutions-go"
)

func main() {
	ctx := context.Background()

	apiKey := os.Getenv("SPEECHREVOLUTIONS_API_KEY")
	client, err := stt.NewClient(apiKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// (Optional) submit a job whose completion is delivered to a webhook instead
	// of waiting: client.Transcribe(ctx, path, stt.TranscribeOptions{CallbackURL: "https://..."}, nil)

	// 1. Fire-and-forget: Submit returns a job id immediately, without waiting.
	jobID, err := client.Submit(ctx, "audio.mp3", stt.TranscribeOptions{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("submitted job %s\n", jobID)

	// (Bonus) list your most-recent jobs (newest first), cursor-paginated.
	if page, lerr := client.ListJobs(ctx, 10, ""); lerr == nil {
		fmt.Printf("%d recent job(s); next_before=%q\n", len(page.Jobs), page.NextBefore)
	}

	// 2. Collect later: poll status, then fetch the transcript by id.
	fmt.Printf("Fetching transcript for %s ...\n", jobID)
	for {
		status, err := client.GetJobStatus(ctx, jobID)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("  status: %s\n", status.Status)
		if status.IsCompleted() {
			result, err := client.GetTranscript(ctx, jobID, stt.OutputJSON)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			text := result.Text()
			if len(text) > 500 {
				text = text[:500]
			}
			fmt.Println(text)
			return
		}
		if status.IsFailed() {
			fmt.Fprintf(os.Stderr, "job failed at %s: %s\n", status.FailedStage, status.Reason)
			os.Exit(1)
		}
		time.Sleep(3 * time.Second)
	}
}
