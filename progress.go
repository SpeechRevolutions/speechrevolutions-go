package stt

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Neither AssemblyAI nor Deepgram surfaces live percentage progress for
// pre-recorded transcription — our pipeline is chunked and emits SSE progress
// events, so live progress is a Speech Revolutions extra. This file holds the
// optional console bar renderer plus the byte-progress upload body. Go has no
// tqdm, so the renderer is a tiny built-in: a single line written to os.Stderr,
// updated in place with a carriage return.

// Minimum time between redraws — a byte upload fires many events.
const minRedrawInterval = 80 * time.Millisecond

// barWidth is the number of cells in the [####----] bar.
const barWidth = 30

// barPrinter renders ProgressEvents to a console bar and forwards each event to
// an optional user callback. A single, stable bar per phase: for transcription
// the volatile step name (preprocess / chunk:N / aggregation) is deliberately
// kept off the bar — chunks finish out of order and made the label jump around;
// callers who want it read event.Step in their callback.
type barPrinter struct {
	forward   ProgressFunc
	label     string
	bytesMode bool // render sizes (2.5MB/6.0MB) instead of a bare bar
	w         io.Writer

	started  bool
	finished bool // terminating newline already written
	closed   bool
	lastDraw time.Time
}

// handle is the ProgressFunc that renders then forwards.
func (b *barPrinter) handle(e ProgressEvent) {
	b.render(e)
	if b.forward != nil {
		b.forward(e)
	}
}

func (b *barPrinter) render(e ProgressEvent) {
	pct, ok := e.Percent()
	if !ok {
		return
	}
	completed, total := 0, 0
	if e.Completed != nil {
		completed = *e.Completed
	}
	if e.Total != nil {
		total = *e.Total
	}
	complete := total > 0 && completed >= total

	// Throttle redraws (uploads emit many events); always draw the first frame
	// and the final 100% frame.
	now := time.Now()
	if !complete && b.started && now.Sub(b.lastDraw) < minRedrawInterval {
		return
	}
	b.started = true
	b.lastDraw = now
	b.draw(pct, completed, total, complete)
}

func (b *barPrinter) draw(pct float64, completed, total int, complete bool) {
	filled := int(float64(barWidth) * pct / 100.0)
	if filled < 0 {
		filled = 0
	}
	if filled > barWidth {
		filled = barWidth
	}
	bar := strings.Repeat("#", filled) + strings.Repeat("-", barWidth-filled)
	if b.bytesMode {
		fmt.Fprintf(b.w, "\r%s: %3.0f%% [%s] %s/%s", b.label, pct, bar, humanBytes(completed), humanBytes(total))
	} else {
		fmt.Fprintf(b.w, "\r%s: %3.0f%% [%s]", b.label, pct, bar)
	}
	if complete {
		fmt.Fprint(b.w, "\n")
		b.finished = true
	}
}

// close finishes the bar. Safe to call more than once. If a bar was drawn but
// never reached 100% (e.g. the transcription "completed" event carried no final
// progress), a final 100% frame is drawn so the line always ends at 100%.
func (b *barPrinter) close() {
	if b == nil || b.closed {
		return
	}
	b.closed = true
	if b.started && !b.finished {
		bar := strings.Repeat("#", barWidth)
		fmt.Fprintf(b.w, "\r%s: 100%% [%s]\n", b.label, bar)
		b.finished = true
	}
}

// resolveProgress builds the effective progress callback for a phase. When show
// is true it wraps user in a barPrinter that renders to the console and still
// forwards to user; the returned printer (may be nil) must have close() called
// when the phase ends.
func resolveProgress(user ProgressFunc, show bool, label string, bytesMode bool) (ProgressFunc, *barPrinter) {
	if !show {
		return user, nil
	}
	p := &barPrinter{forward: user, label: label, bytesMode: bytesMode, w: os.Stderr}
	return p.handle, p
}

func humanBytes(n int) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	f := float64(n)
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	i := 0
	for f >= unit && i < len(units)-1 {
		f /= unit
		i++
	}
	return fmt.Sprintf("%.1f%s", f, units[i])
}

// byteProgressAdapter turns a ProgressFunc into a (sent, total) byte callback.
// Upload events are reported as ProgressEvent{Step: "upload"} so they share the
// same shape (and Percent()) as transcription progress.
func byteProgressAdapter(fn ProgressFunc) func(sent, total int) {
	if fn == nil {
		return nil
	}
	return func(sent, total int) {
		s, t := sent, total
		fn(ProgressEvent{Completed: &s, Total: &t, Step: "upload"})
	}
}

// progressReader is an io.Reader view over in-memory bytes that reports read
// progress after each Read. Used as the streamed PUT/POST body so net/http can
// observe upload progress while a correct Content-Length is set explicitly on
// the request (presigned S3 PUTs reject Transfer-Encoding: chunked). A fresh
// reader is created per upload attempt, so a retry restarts progress from 0.
type progressReader struct {
	data []byte
	pos  int
	cb   func(sent, total int)
}

func (r *progressReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.cb != nil && n > 0 {
		r.cb(r.pos, len(r.data))
	}
	return n, nil
}
