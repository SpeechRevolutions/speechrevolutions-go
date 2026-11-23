package stt

// Real-time streaming transcription over WebSocket (wss://.../api/v1/stream).
//
// A LiveSession, per the streaming protocol:
//   - buffers every chunk until the server acknowledges it (audio.committed), at most ~30 s;
//   - resumes a dropped connection: it reconnects with ?job_id= for up to 60 s (at once on close
//     code 1012, "going away for a deploy"), then sends rewind and replays the buffer;
//   - replays on failover (resend) the same way, paced at up to 3x real time.
//
// Audio is PCM16 little-endian, mono, 16 kHz. LoadPCM16 converts any file (ffmpeg for anything
// that isn't already a 16 kHz mono 16-bit WAV).

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	streamSampleRate = 16000
	bytesPerMs       = streamSampleRate * 2 / 1000 // 32
	chunkMs          = 80
	replaySpeed      = 3
	resumeWindow     = 60 * time.Second
	pingInterval     = 10 * time.Second
	// MaxLead is how far ahead of real time StreamFile sends audio. The server accepts audio at
	// most ~10 s ahead of the clock and refuses a faster sender with "use_batch".
	MaxLead = 8 * time.Second
)

var fatalStreamErrors = map[string]bool{
	"invalid_api_key": true, "insufficient_balance": true, "session_expired": true, "no_capacity": true,
	"idle_timeout": true, "max_duration": true, "use_batch": true, "frame_too_large": true,
	"bad_audio_format": true,
}

// StreamError is a live session ending with an error. Code is the server's error code, if any.
type StreamError struct {
	Code    string
	Message string
}

func (e *StreamError) Error() string {
	if e.Code != "" && !strings.Contains(e.Message, e.Code) {
		return fmt.Sprintf("stream: %s (%s)", e.Message, e.Code)
	}
	return "stream: " + e.Message
}

// StreamEvent is one server event. Type is the protocol name: session.created, session.resumed,
// transcription.partial (the open utterance so far; it replaces the previous partial of that
// Index; Text[:Stable] is settled), final, final.revised, speech.started, turn.ended,
// language.detected, language.unsupported, audio.committed, transcription.done, error.
type StreamEvent struct {
	Type        string
	Index       *int
	Text        string
	Start       *float64
	End         *float64
	Stable      *int
	AudioS      *float64
	Language    string
	Probability *float64
	Forced      *bool
	JobID       string
	Code        string
	Message     string
	Raw         map[string]any
}

// StreamFinal is one utterance's latest text.
type StreamFinal struct {
	Index int
	Text  string
	Start *float64
	End   *float64
}

// StreamResult is what a finished session produced.
type StreamResult struct {
	JobID string
	// Text is all finals joined, as the server sent it in transcription.done.
	Text string
	// Finals holds each utterance's latest text (final.revised applied), in order.
	Finals []StreamFinal
	// Complete is false when the session ended before transcription.done.
	Complete bool
}

// TurnDetection tunes when a turn ends.
type TurnDetection struct {
	// Threshold is the end-of-turn model's threshold, 0.05–0.99.
	Threshold *float64 `json:"threshold,omitempty"`
	// MaxSilenceMs ends a turn after this much silence regardless, 300–5000.
	MaxSilenceMs *int `json:"max_silence_ms,omitempty"`
}

// StreamOptions configures a live session. The zero value detects the language.
type StreamOptions struct {
	// Language is the spoken language ("en", "ja"...). Empty detects it automatically.
	Language string
	// Languages narrows automatic detection to these languages.
	Languages []string
	// Vocabulary holds names and terms to recognise (included in the streaming price).
	Vocabulary []string
	// TurnDetection tunes when a turn ends.
	TurnDetection *TurnDetection
	// OnEvent is called for every server event, from the session's reader goroutine. Don't block in it.
	OnEvent func(StreamEvent)
	// Extra holds other session.update fields, sent as given.
	Extra map[string]any
}

func (o StreamOptions) sessionUpdate() map[string]any {
	m := map[string]any{
		"type":         "session.update",
		"audio_format": map[string]any{"encoding": "pcm_s16le", "sample_rate": streamSampleRate, "channels": 1},
	}
	if o.Language != "" && o.Language != "auto" {
		m["language"] = o.Language
	}
	if len(o.Languages) > 0 {
		m["languages"] = o.Languages
	}
	if len(o.Vocabulary) > 0 {
		m["vocabulary"] = o.Vocabulary
	}
	if o.TurnDetection != nil {
		m["turn_detection"] = o.TurnDetection
	}
	for k, v := range o.Extra {
		m[k] = v
	}
	return m
}

type pcmChunk struct {
	start int // ms on the job timeline
	data  []byte
}

// LiveSession is a real-time transcription session. Create one with Client.Stream; send audio
// with SendAudio as it is produced, and call Finish at the end of the audio.
type LiveSession struct {
	// JobID is the session's job id (the billing id, and the resume handle).
	JobID string

	url     string
	headers http.Header
	update  map[string]any
	onEvent func(StreamEvent)

	mu          sync.Mutex
	conn        *websocket.Conn
	gen         int
	live        bool // false between a drop and session.resumed: audio waits in the buffer
	buffer      []pcmChunk
	audioMs     int
	nextMs      int
	replayUntil int
	committedMs int
	finishing   bool
	finalSent   bool
	done        *StreamEvent
	fatal       error
	finals      map[int]StreamFinal
	closed      bool
	closedCh    chan struct{}
	wake        chan struct{}
	writeMu     sync.Mutex
	stopPing    context.CancelFunc
}

func streamURL(base string) string {
	base = strings.TrimRight(base, "/")
	if strings.HasPrefix(base, "https://") {
		base = "wss://" + strings.TrimPrefix(base, "https://")
	} else if strings.HasPrefix(base, "http://") {
		base = "ws://" + strings.TrimPrefix(base, "http://")
	}
	return base + "/api/v1/stream"
}

// Stream opens a real-time transcription session and returns once the server has started it.
func (c *Client) Stream(ctx context.Context, opts StreamOptions) (*LiveSession, error) {
	h := http.Header{}
	h.Set("X-API-Key", c.APIKey)
	h.Set("User-Agent", userAgent)
	s := &LiveSession{
		url:      streamURL(c.BaseURL),
		headers:  h,
		update:   opts.sessionUpdate(),
		onEvent:  opts.OnEvent,
		finals:   map[int]StreamFinal{},
		closedCh: make(chan struct{}),
		wake:     make(chan struct{}, 1),
	}
	if err := s.open(ctx, false); err != nil {
		return nil, err
	}
	go s.sendLoop()
	return s, nil
}

// StreamFile streams a file as if it were live audio, in real time (the server takes audio at
// most ~10 s ahead of the clock). For testing a streaming integration; for files, Transcribe is
// faster and cheaper.
func (c *Client) StreamFile(ctx context.Context, path string, opts StreamOptions) (*StreamResult, error) {
	pcm, err := LoadPCM16(path)
	if err != nil {
		return nil, err
	}
	s, err := c.Stream(ctx, opts)
	if err != nil {
		return nil, err
	}
	return StreamPCM(ctx, s, pcm)
}

// StreamPCM sends recorded PCM through s as live audio, then finishes. Audio goes out in real
// time, up to MaxLead ahead of the clock, so the first seconds go at once.
func StreamPCM(ctx context.Context, s *LiveSession, pcm []byte) (*StreamResult, error) {
	step := chunkMs * bytesPerMs
	t0 := time.Now()
	for i, n := 0, 0; i < len(pcm); i, n = i+step, n+1 {
		end := i + step
		if end > len(pcm) {
			end = len(pcm)
		}
		if err := s.SendAudio(pcm[i:end]); err != nil {
			return nil, err
		}
		due := t0.Add(time.Duration(n+1)*chunkMs*time.Millisecond - MaxLead)
		if wait := time.Until(due); wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				s.Close()
				return nil, ctx.Err()
			}
		}
	}
	return s.Finish(ctx)
}

// SendAudio queues PCM16 mono 16 kHz audio. Send it at the pace it is produced.
func (s *LiveSession) SendAudio(pcm []byte) error {
	if len(pcm)%2 != 0 {
		return errors.New("stream: PCM16 audio must be an even number of bytes")
	}
	s.mu.Lock()
	if s.fatal != nil {
		err := s.fatal
		s.mu.Unlock()
		return err
	}
	if s.finishing {
		s.mu.Unlock()
		return errors.New("stream: SendAudio after Finish")
	}
	if len(pcm) > 0 {
		s.buffer = append(s.buffer, pcmChunk{start: s.audioMs, data: append([]byte(nil), pcm...)})
		s.audioMs += len(pcm) / bytesPerMs
	}
	s.mu.Unlock()
	s.poke()
	return nil
}

// EndTurn ends the current turn now (push-to-talk release).
func (s *LiveSession) EndTurn() error {
	return s.writeJSON(map[string]any{"type": "turn.end"})
}

// Finish ends the audio, waits for transcription.done, and returns the result. It waits at most
// until ctx is done, or two minutes when ctx has no deadline; the result then has Complete false.
func (s *LiveSession) Finish(ctx context.Context) (*StreamResult, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
	}
	s.mu.Lock()
	s.finishing = true
	s.mu.Unlock()
	s.poke()
	select {
	case <-s.closedCh:
	case <-ctx.Done():
		s.Close()
		return s.result(false), ctx.Err()
	}
	s.teardown()
	s.mu.Lock()
	fatal, done := s.fatal, s.done
	s.mu.Unlock()
	if fatal != nil && done == nil {
		return s.result(false), fatal
	}
	return s.result(done != nil), nil
}

// Close drops the connection without waiting for the transcript.
func (s *LiveSession) Close() error {
	s.markClosed()
	s.teardown()
	return nil
}

// simulateDrop cuts the connection without a close handshake (tests).
func (s *LiveSession) simulateDrop() {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if c != nil {
		c.CloseNow()
	}
}

// ------------------------------------------------------------------ connection

func (s *LiveSession) open(ctx context.Context, resume bool) error {
	u := s.url
	if resume && s.JobID != "" {
		u += "?job_id=" + url.QueryEscape(s.JobID)
	}
	dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(dctx, u, &websocket.DialOptions{HTTPHeader: s.headers})
	if err != nil {
		if resp != nil {
			switch resp.StatusCode {
			case 401, 403:
				return &StreamError{Code: "invalid_api_key", Message: fmt.Sprintf("refused (HTTP %d): check the API key", resp.StatusCode)}
			case 429:
				return &StreamError{Code: "rate_limited", Message: "refused (HTTP 429): too many sessions or connects"}
			}
			return &StreamError{Message: fmt.Sprintf("refused (HTTP %d)", resp.StatusCode)}
		}
		return &StreamError{Message: "connection failed: " + err.Error()}
	}
	conn.SetReadLimit(1 << 24)

	s.mu.Lock()
	s.gen++
	gen := s.gen
	s.conn = conn
	s.mu.Unlock()

	if !resume {
		if err := s.writeJSON(s.update); err != nil {
			conn.CloseNow()
			return &StreamError{Message: "could not send session.update: " + err.Error()}
		}
		if err := s.writeJSON(map[string]any{"type": "input_audio_buffer.commit"}); err != nil { // start of audio
			conn.CloseNow()
			return &StreamError{Message: "could not start the audio: " + err.Error()}
		}
	}
	// Read until session.created / session.resumed.
	rctx, rcancel := context.WithTimeout(ctx, 15*time.Second)
	defer rcancel()
	for {
		_, data, err := conn.Read(rctx)
		if err != nil {
			conn.CloseNow()
			return &StreamError{Message: "closed before the session started: " + err.Error()}
		}
		ev, ok := parseStreamEvent(data)
		if !ok {
			continue
		}
		if ev.Type == "error" {
			conn.Close(websocket.StatusNormalClosure, "")
			return streamErrorOf(ev)
		}
		s.handle(ev)
		if ev.Type == "session.created" || ev.Type == "session.resumed" {
			break
		}
	}
	pctx, pcancel := context.WithCancel(context.Background())
	s.mu.Lock()
	if s.stopPing != nil {
		s.stopPing()
	}
	s.stopPing = pcancel
	s.mu.Unlock()
	go s.pingLoop(pctx, conn)
	go s.readLoop(conn, gen)
	return nil
}

func (s *LiveSession) pingLoop(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, 2*pingInterval)
			_ = conn.Ping(pctx)
			cancel()
		}
	}
}

func (s *LiveSession) readLoop(conn *websocket.Conn, gen int) {
	for {
		_, data, err := conn.Read(context.Background())
		if err != nil {
			s.mu.Lock()
			current := gen == s.gen
			closed := s.closed
			if current {
				s.live = false
			}
			s.mu.Unlock()
			if !current || closed {
				return
			}
			s.resume(websocket.CloseStatus(err) == websocket.StatusServiceRestart)
			return
		}
		ev, ok := parseStreamEvent(data)
		if !ok {
			continue
		}
		s.handle(ev)
		switch {
		case ev.Type == "transcription.done":
			s.markClosed()
			conn.Close(websocket.StatusNormalClosure, "")
			return
		case ev.Type == "error":
			s.mu.Lock()
			terminal := fatalStreamErrors[ev.Code] || s.JobID == ""
			if terminal {
				s.fatal = streamErrorOf(ev)
			}
			s.mu.Unlock()
			if terminal {
				s.markClosed()
				return
			}
		}
	}
}

func (s *LiveSession) resume(immediately bool) {
	deadline := time.Now().Add(resumeWindow)
	delay := 500 * time.Millisecond
	if immediately {
		delay = 0
	}
	for {
		s.mu.Lock()
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		err := s.open(context.Background(), true)
		if err == nil {
			return
		}
		var se *StreamError
		if errors.As(err, &se) && fatalStreamErrors[se.Code] {
			s.setFatal(err)
			break
		}
		if time.Now().After(deadline) {
			s.setFatal(&StreamError{Message: "connection lost and could not be resumed within 60 s"})
			break
		}
		delay *= 2
		if delay < 500*time.Millisecond {
			delay = 500 * time.Millisecond
		}
		if delay > 5*time.Second {
			delay = 5 * time.Second
		}
	}
	s.markClosed()
}

func (s *LiveSession) setFatal(err error) {
	s.mu.Lock()
	s.fatal = err
	s.mu.Unlock()
}

// ------------------------------------------------------------------ events

func (s *LiveSession) handle(ev StreamEvent) {
	s.mu.Lock()
	switch ev.Type {
	case "session.created":
		s.JobID = ev.JobID
		s.live = true
	case "session.resumed", "resend":
		from := s.committedMs
		if v, ok := ev.Raw["resume_from_ms"].(float64); ok {
			from = int(v)
		} else if v, ok := ev.Raw["from_ms"].(float64); ok {
			from = int(v)
		}
		s.mu.Unlock()
		_ = s.writeJSON(map[string]any{"type": "input_audio_buffer.rewind", "from_ms": from})
		s.mu.Lock()
		if s.nextMs > s.replayUntil {
			s.replayUntil = s.nextMs
		}
		s.nextMs = from
		s.finalSent = false // the commit is re-sent once the replay is through
		s.live = true
	case "audio.committed":
		if v, ok := ev.Raw["through_ms"].(float64); ok {
			s.committedMs = int(v)
		}
		kept := s.buffer[:0]
		for _, c := range s.buffer {
			if c.start+len(c.data)/bytesPerMs > s.committedMs {
				kept = append(kept, c)
			}
		}
		s.buffer = kept
	case "final":
		if ev.Index != nil {
			s.finals[*ev.Index] = StreamFinal{Index: *ev.Index, Text: ev.Text, Start: ev.Start, End: ev.End}
		}
	case "final.revised":
		if ev.Index != nil {
			f := s.finals[*ev.Index]
			s.finals[*ev.Index] = StreamFinal{Index: *ev.Index, Text: ev.Text, Start: f.Start, End: f.End}
		}
	case "transcription.done":
		e := ev
		s.done = &e
	}
	s.mu.Unlock()
	s.poke()
	if ev.Type != "resend" && s.onEvent != nil {
		s.onEvent(ev)
	}
}

func parseStreamEvent(data []byte) (StreamEvent, bool) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return StreamEvent{}, false
	}
	str := func(k string) string { v, _ := m[k].(string); return v }
	fptr := func(k string) *float64 {
		if v, ok := m[k].(float64); ok {
			return &v
		}
		return nil
	}
	iptr := func(k string) *int {
		if v, ok := m[k].(float64); ok {
			i := int(v)
			return &i
		}
		return nil
	}
	ev := StreamEvent{
		Type: str("type"), Index: iptr("index"), Text: str("text"), Start: fptr("start"), End: fptr("end"),
		Stable: iptr("stable"), AudioS: fptr("audio_s"), Language: str("language"),
		Probability: fptr("probability"), JobID: str("job_id"), Code: str("code"), Message: str("message"), Raw: m,
	}
	if v, ok := m["forced"].(bool); ok {
		ev.Forced = &v
	}
	if e := str("error"); e != "" {
		if ev.Message == "" {
			ev.Message = e
		}
		if ev.Code == "" && !strings.Contains(e, " ") {
			ev.Code = e
		}
	}
	return ev, true
}

func streamErrorOf(ev StreamEvent) error {
	msg := ev.Message
	if msg == "" {
		msg = ev.Code
	}
	if msg == "" {
		msg = "stream error"
	}
	return &StreamError{Code: ev.Code, Message: msg}
}

// ------------------------------------------------------------------ sending

func (s *LiveSession) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *LiveSession) writeJSON(m map[string]any) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if c == nil {
		return errors.New("stream: not connected")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.Write(ctx, websocket.MessageText, b)
}

func (s *LiveSession) sendLoop() {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		var chunk *pcmChunk
		if s.live {
			chunk = s.chunkAt(s.nextMs)
		}
		sendFinal := s.live && chunk == nil && s.finishing && !s.finalSent && s.nextMs >= s.audioMs
		if sendFinal {
			s.finalSent = true
		}
		var replay bool
		if chunk != nil {
			replay = chunk.start < s.replayUntil
			s.nextMs = chunk.start + len(chunk.data)/bytesPerMs
		}
		s.mu.Unlock()

		switch {
		case chunk != nil:
			// A failed write leaves the audio in the buffer; the reader notices the drop and resumes.
			_ = s.writeJSON(map[string]any{"type": "input_audio_buffer.append", "audio": base64.StdEncoding.EncodeToString(chunk.data)})
			if replay {
				time.Sleep(time.Duration(len(chunk.data)/bytesPerMs) * time.Millisecond / replaySpeed)
			}
		case sendFinal:
			_ = s.writeJSON(map[string]any{"type": "input_audio_buffer.commit", "final": true})
		default:
			select {
			case <-s.wake:
			case <-s.closedCh:
				return
			}
		}
	}
}

// chunkAt returns up to chunkMs of buffered audio starting at ms (caller holds mu).
func (s *LiveSession) chunkAt(ms int) *pcmChunk {
	for _, c := range s.buffer {
		end := c.start + len(c.data)/bytesPerMs
		if end <= ms {
			continue
		}
		off := 0
		if ms > c.start {
			off = (ms - c.start) * bytesPerMs
		}
		stop := off + chunkMs*bytesPerMs
		if stop > len(c.data) {
			stop = len(c.data)
		}
		if off >= stop {
			return nil
		}
		return &pcmChunk{start: c.start + off/bytesPerMs, data: c.data[off:stop]}
	}
	return nil
}

// ------------------------------------------------------------------ end

func (s *LiveSession) markClosed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.closedCh)
	}
}

func (s *LiveSession) teardown() {
	s.mu.Lock()
	s.gen++ // late frames from this connection are ignored
	c := s.conn
	if s.stopPing != nil {
		s.stopPing()
	}
	s.mu.Unlock()
	if c != nil {
		c.Close(websocket.StatusNormalClosure, "")
	}
}

func (s *LiveSession) result(complete bool) *StreamResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]int, 0, len(s.finals))
	for k := range s.finals {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	finals := make([]StreamFinal, 0, len(keys))
	texts := make([]string, 0, len(keys))
	for _, k := range keys {
		finals = append(finals, s.finals[k])
		if s.finals[k].Text != "" {
			texts = append(texts, s.finals[k].Text)
		}
	}
	text := strings.Join(texts, " ")
	if s.done != nil {
		text = s.done.Text
	}
	return &StreamResult{JobID: s.JobID, Text: text, Finals: finals, Complete: complete}
}

// LoadPCM16 reads any audio file as PCM16 mono 16 kHz: directly if it already is that WAV,
// otherwise through ffmpeg.
func LoadPCM16(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if pcm, ok := wavPCM16Mono16k(data); ok {
		return pcm, nil
	}
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return nil, errors.New("stream: ffmpeg is needed to convert this file to PCM16 mono 16 kHz (or pass such a WAV)")
	}
	var out, errb bytes.Buffer
	cmd := exec.Command("ffmpeg", "-v", "error", "-i", path, "-f", "s16le", "-ac", "1", "-ar", "16000", "-")
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil || out.Len() == 0 {
		msg := errb.String()
		if len(msg) > 300 {
			msg = msg[:300]
		}
		return nil, fmt.Errorf("stream: ffmpeg could not decode %s: %s", path, strings.TrimSpace(msg))
	}
	return out.Bytes(), nil
}

func wavPCM16Mono16k(b []byte) ([]byte, bool) {
	if len(b) < 44 || string(b[0:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		return nil, false
	}
	ok := false
	for o := 12; o+8 <= len(b); {
		id := string(b[o : o+4])
		size := int(binary.LittleEndian.Uint32(b[o+4 : o+8]))
		if id == "fmt " && o+24 <= len(b) {
			format := binary.LittleEndian.Uint16(b[o+8:])
			channels := binary.LittleEndian.Uint16(b[o+10:])
			rate := binary.LittleEndian.Uint32(b[o+12:])
			bits := binary.LittleEndian.Uint16(b[o+22:])
			ok = format == 1 && channels == 1 && rate == streamSampleRate && bits == 16
		} else if id == "data" && ok {
			end := o + 8 + size
			if end > len(b) {
				end = len(b)
			}
			return b[o+8 : end], true
		}
		o += 8 + size + size%2
	}
	return nil, false
}
