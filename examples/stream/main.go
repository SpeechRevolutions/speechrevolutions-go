// Stream an audio file as if it were live, printing partials and finals as they arrive.
//
//	SPEECHREVOLUTIONS_API_KEY=... go run ./examples/stream call.mp3 en
//
// Leave out the language to have it detected. Needs ffmpeg for anything other than a 16 kHz mono
// 16-bit WAV. The file streams in real time, as live audio would.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	stt "github.com/speechrevolutions/speechrevolutions-go"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: stream <audio file> [language]")
	}
	lang := ""
	if len(os.Args) > 2 {
		lang = os.Args[2]
	}
	client, err := stt.NewClient("") // reads SPEECHREVOLUTIONS_API_KEY
	if err != nil {
		log.Fatal(err)
	}
	res, err := client.StreamFile(context.Background(), os.Args[1], stt.StreamOptions{
		Language: lang,
		OnEvent: func(e stt.StreamEvent) {
			switch e.Type {
			case "session.created":
				fmt.Println("session", e.JobID)
			case "transcription.partial":
				fmt.Printf("\r\033[K%s", e.Text) // redraw the open utterance
			case "final":
				fmt.Printf("\r\033[K[%6.2f-%6.2f] %s\n", *e.Start, *e.End, e.Text)
			case "language.detected":
				fmt.Println("   language:", e.Language)
			}
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n%d finals, job %s\n%s\n", len(res.Finals), res.JobID, res.Text)
}
