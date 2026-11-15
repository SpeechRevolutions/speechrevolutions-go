package stt

// Realtime streaming transcription (WebSocket /api/v1/stream).
//
//	s, err := client.Stream(ctx, stt.StreamOptions{}) // empty Language = detect it
//	go func() { for chunk := range pcm16Chunks { s.SendAudio(ctx, chunk) }; s.Finish(ctx) }()
//	for {
//		ev, err := s.Next(ctx)
//		if err != nil { break }             // io.EOF after "done"
//		if ev.Type == "final" { fmt.Println(ev.Start, ev.Text) }
//	}
//
// Audio is PCM16 mono 16 kHz. Event types: partial (replaces the previous partial of the same Index;
// Stable = settled leading characters), final, revision, speech_started, turn_ended, language, done, error.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
)

// StreamOptions configures a realtime session.
type StreamOptions struct {
	Language      string   // ISO code to pin the language; "" detects it
	Vocabulary    []string // terms to bias recognition toward
	EndUserID     string   // your id for the speaker: their learned vocabulary is added (opted-in accounts)
	TurnThreshold *float64 // end-of-turn probability threshold (Smart Turn)
	MaxSilenceMs  *int     // silence after which a turn always ends
}

// StreamEvent is one server event, normalised; Raw holds the original message.
type StreamEvent struct {
	Type     string
	Text     string
	Index    *int
	Start    *float64
	End      *float64
	Stable   *int
	Language string
	Raw      map[string]any
}

var streamTypes = map[string]string{
	"transcription.partial": "partial", "final": "final", "final.revised": "revision",
	"speech.started": "speech_started", "turn.ended": "turn_ended", "language.detected": "language",
	"transcription.done": "done", "error": "error", "session.created": "session",
}

func (o StreamOptions) sessionUpdate() map[string]any {
	u := map[string]any{"type": "session.update"}
	if o.Language != "" && o.Language != "auto" {
		u["language"] = o.Language
	}
	if len(o.Vocabulary) > 0 {
		u["vocabulary"] = o.Vocabulary
	}
	if o.EndUserID != "" {
		u["end_user_id"] = o.EndUserID
	}
	turn := map[string]any{}
	if o.TurnThreshold != nil {
		turn["threshold"] = *o.TurnThreshold
	}
	if o.MaxSilenceMs != nil {
		turn["max_silence_ms"] = *o.MaxSilenceMs
	}
	if len(turn) > 0 {
		u["turn_detection"] = turn
	}
	return u
}

func toStreamEvent(m map[string]any) StreamEvent {
	raw, _ := m["type"].(string)
	t, ok := streamTypes[raw]
	if !ok {
		t = raw
	}
	str := func(k string) string { s, _ := m[k].(string); return s }
	num := func(k string) *float64 {
		if f, ok := m[k].(float64); ok {
			return &f
		}
		return nil
	}
	integer := func(k string) *int {
		if f, ok := m[k].(float64); ok {
			i := int(f)
			return &i
		}
		return nil
	}
	text := str("text")
	if t == "error" && text == "" {
		if text = str("error"); text == "" {
			text = str("message")
		}
	}
	start := num("start")
	if start == nil {
		start = num("audio_s")
	}
	return StreamEvent{Type: t, Text: text, Index: integer("index"), Start: start, End: num("end"),
		Stable: integer("stable"), Language: str("language"), Raw: m}
}

// StreamSession is a live transcription session. SendAudio may be called from one goroutine while another calls Next.
type StreamSession struct {
	conn  *websocket.Conn
	mu    sync.Mutex
	done  bool
	JobID string // the job the stream is recorded (and billed) under, once the server has said
}

// Stream opens a realtime transcription session.
func (c *Client) Stream(ctx context.Context, opts StreamOptions) (*StreamSession, error) {
	url := strings.Replace(strings.TrimRight(c.BaseURL, "/"), "http", "ws", 1) + "/api/v1/stream"
	h := http.Header{}
	h.Set("X-API-Key", c.APIKey)
	h.Set("User-Agent", userAgent) // the edge 403s a handshake without one
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: h})
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(-1)
	s := &StreamSession{conn: conn}
	if err := s.send(ctx, opts.sessionUpdate()); err != nil {
		conn.CloseNow()
		return nil, err
	}
	if err := s.send(ctx, map[string]any{"type": "input_audio_buffer.commit"}); err != nil {
		conn.CloseNow()
		return nil, err
	}
	return s, nil
}

// SendAudio sends PCM16 mono 16 kHz audio (any chunk size).
func (s *StreamSession) SendAudio(ctx context.Context, pcm16 []byte) error {
	if len(pcm16) == 0 {
		return nil
	}
	return s.send(ctx, map[string]any{"type": "input_audio_buffer.append", "audio": base64.StdEncoding.EncodeToString(pcm16)})
}

// EndTurn finalises the current utterance now (push-to-talk release).
func (s *StreamSession) EndTurn(ctx context.Context) error {
	return s.send(ctx, map[string]any{"type": "turn.end"})
}

// Finish says there is no more audio: the server sends the remaining finals, then "done".
func (s *StreamSession) Finish(ctx context.Context) error {
	return s.send(ctx, map[string]any{"type": "input_audio_buffer.commit", "final": true})
}

// Next returns the next event; io.EOF after "done" or a closed connection. An "error" event is returned once,
// then io.EOF.
func (s *StreamSession) Next(ctx context.Context) (StreamEvent, error) {
	for {
		if s.done {
			return StreamEvent{}, io.EOF
		}
		_, data, err := s.conn.Read(ctx)
		if err != nil {
			var ce websocket.CloseError
			if errors.As(err, &ce) {
				return StreamEvent{}, io.EOF
			}
			return StreamEvent{}, err
		}
		var m map[string]any
		if json.Unmarshal(data, &m) != nil {
			continue
		}
		ev := toStreamEvent(m)
		if ev.Type == "session" {
			s.JobID, _ = m["job_id"].(string)
			continue
		}
		if ev.Type == "done" || ev.Type == "error" {
			s.done = true
		}
		return ev, nil
	}
}

// Close ends the session.
func (s *StreamSession) Close() error {
	return s.conn.Close(websocket.StatusNormalClosure, "")
}

func (s *StreamSession) send(ctx context.Context, m map[string]any) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Write(ctx, websocket.MessageText, b)
}
