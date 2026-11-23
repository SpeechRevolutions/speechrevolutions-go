package stt

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func testPCM(ms int, v int16) []byte {
	b := make([]byte, ms*bytesPerMs)
	for i := 0; i < len(b); i += 2 {
		binary.LittleEndian.PutUint16(b[i:], uint16(v))
	}
	return b
}

// fakeConn is one connection to the fake server, with everything the client sent.
type fakeConn struct {
	url  string
	hdr  http.Header
	ws   *websocket.Conn
	mu   sync.Mutex
	got  []map[string]any
	cond *sync.Cond
}

func (c *fakeConn) send(t *testing.T, m map[string]any) {
	b, _ := json.Marshal(m)
	_ = c.ws.Write(context.Background(), websocket.MessageText, b)
}

func (c *fakeConn) audio(from int) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []byte
	for _, m := range c.got[from:] {
		if m["type"] == "input_audio_buffer.append" {
			b, _ := base64.StdEncoding.DecodeString(m["audio"].(string))
			out = append(out, b...)
		}
	}
	return out
}

func (c *fakeConn) waitFor(cond func() bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for !cond() {
		c.cond.Wait()
	}
}

func (c *fakeConn) hasFinalCommit() bool {
	for _, m := range c.got {
		if m["type"] == "input_audio_buffer.commit" && m["final"] == true {
			return true
		}
	}
	return false
}

func (c *fakeConn) audioLocked() int {
	n := 0
	for _, m := range c.got {
		if m["type"] == "input_audio_buffer.append" {
			b, _ := base64.StdEncoding.DecodeString(m["audio"].(string))
			n += len(b)
		}
	}
	return n
}

type fakeStreamServer struct {
	srv   *httptest.Server
	mu    sync.Mutex
	conns []*fakeConn
}

func newFakeStreamServer(t *testing.T, script func(n int, c *fakeConn)) *fakeStreamServer {
	f := &fakeStreamServer{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		c := &fakeConn{url: r.URL.RequestURI(), hdr: r.Header.Clone(), ws: ws}
		c.cond = sync.NewCond(&c.mu)
		f.mu.Lock()
		f.conns = append(f.conns, c)
		n := len(f.conns)
		f.mu.Unlock()
		go func() {
			for {
				_, data, err := ws.Read(context.Background())
				if err != nil {
					c.mu.Lock()
					c.cond.Broadcast()
					c.mu.Unlock()
					return
				}
				var m map[string]any
				_ = json.Unmarshal(data, &m)
				c.mu.Lock()
				c.got = append(c.got, m)
				c.cond.Broadcast()
				c.mu.Unlock()
			}
		}()
		script(n, c)
		time.Sleep(100 * time.Millisecond)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeStreamServer) conn(i int) *fakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conns[i]
}

func finishNormally(t *testing.T, c *fakeConn) {
	c.waitFor(c.hasFinalCommit)
	c.send(t, map[string]any{"type": "final", "index": 0, "text": "hello world", "start": 0, "end": 1})
	c.send(t, map[string]any{"type": "transcription.done", "text": "hello world"})
}

func testClient(t *testing.T, base string) *Client {
	c, err := NewClient("k")
	if err != nil {
		t.Fatal(err)
	}
	c.BaseURL = base
	return c
}

func TestStreamNormalSession(t *testing.T) {
	f := newFakeStreamServer(t, func(n int, c *fakeConn) {
		c.send(t, map[string]any{"type": "session.created", "job_id": "job-1"})
		finishNormally(t, c)
	})
	thr := 0.5
	s, err := testClient(t, f.srv.URL).Stream(context.Background(), StreamOptions{
		Language: "en", Vocabulary: []string{"Zephyr"}, TurnDetection: &TurnDetection{Threshold: &thr},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SendAudio(testPCM(400, 1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := s.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := f.conn(0)
	if c.url != "/api/v1/stream" || c.hdr.Get("X-API-Key") != "k" {
		t.Fatalf("url %q key %q", c.url, c.hdr.Get("X-API-Key"))
	}
	upd := c.got[0]
	if upd["type"] != "session.update" || upd["language"] != "en" {
		t.Fatalf("update %v", upd)
	}
	if td := upd["turn_detection"].(map[string]any); td["threshold"] != 0.5 {
		t.Fatalf("turn_detection %v", td)
	}
	if c.got[1]["type"] != "input_audio_buffer.commit" || c.got[1]["final"] != nil {
		t.Fatalf("start commit %v", c.got[1])
	}
	if !bytes.Equal(c.audio(0), testPCM(400, 1)) {
		t.Fatal("audio differs")
	}
	if !res.Complete || res.Text != "hello world" || res.JobID != "job-1" {
		t.Fatalf("result %+v", res)
	}
}

func TestStreamResumeReplaysUnacknowledgedAudio(t *testing.T) {
	dropped := make(chan struct{})
	f := newFakeStreamServer(t, func(n int, c *fakeConn) {
		if n == 1 {
			c.send(t, map[string]any{"type": "session.created", "job_id": "job-r"})
			c.waitFor(func() bool { return c.audioLocked() >= 600*bytesPerMs })
			c.send(t, map[string]any{"type": "audio.committed", "through_ms": 200})
			time.Sleep(50 * time.Millisecond)
			close(dropped)
			c.ws.Close(websocket.StatusServiceRestart, "deploy")
			return
		}
		c.send(t, map[string]any{"type": "session.resumed", "job_id": "job-r", "resume_from_ms": 200})
		finishNormally(t, c)
	})
	s, err := testClient(t, f.srv.URL).Stream(context.Background(), StreamOptions{Language: "en"})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.SendAudio(testPCM(600, 7))
	<-dropped
	time.Sleep(200 * time.Millisecond)
	_ = s.SendAudio(testPCM(200, 9))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := s.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c2 := f.conn(1)
	if c2.url != "/api/v1/stream?job_id=job-r" {
		t.Fatalf("resume url %q", c2.url)
	}
	if c2.got[0]["type"] != "input_audio_buffer.rewind" || c2.got[0]["from_ms"] != float64(200) {
		t.Fatalf("first message after resume %v", c2.got[0])
	}
	want := append(testPCM(400, 7), testPCM(200, 9)...)
	if !bytes.Equal(c2.audio(0), want) {
		t.Fatalf("replayed %d bytes, want %d", len(c2.audio(0)), len(want))
	}
	if !res.Complete || res.JobID != "job-r" {
		t.Fatalf("result %+v", res)
	}
}

func TestStreamFailoverResend(t *testing.T) {
	f := newFakeStreamServer(t, func(n int, c *fakeConn) {
		c.send(t, map[string]any{"type": "session.created", "job_id": "j"})
		c.waitFor(func() bool { return c.audioLocked() >= 480*bytesPerMs })
		c.send(t, map[string]any{"type": "audio.committed", "through_ms": 160})
		c.send(t, map[string]any{"type": "resend", "from_ms": 160})
		finishNormally(t, c)
	})
	s, err := testClient(t, f.srv.URL).Stream(context.Background(), StreamOptions{Language: "en"})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.SendAudio(testPCM(480, 3))
	time.Sleep(300 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.Finish(ctx); err != nil {
		t.Fatal(err)
	}
	c := f.conn(0)
	rw := -1
	for i, m := range c.got {
		if m["type"] == "input_audio_buffer.rewind" {
			rw = i
		}
	}
	if rw < 0 || c.got[rw]["from_ms"] != float64(160) {
		t.Fatal("no rewind to 160")
	}
	if !bytes.Equal(c.audio(rw), testPCM(320, 3)) {
		t.Fatalf("replay %d bytes", len(c.audio(rw)))
	}
}

func TestStreamFatalErrorAtConnect(t *testing.T) {
	f := newFakeStreamServer(t, func(n int, c *fakeConn) {
		c.send(t, map[string]any{"type": "error", "code": "insufficient_balance", "message": "top up"})
	})
	_, err := testClient(t, f.srv.URL).Stream(context.Background(), StreamOptions{Language: "en"})
	var se *StreamError
	if !errors.As(err, &se) || se.Code != "insufficient_balance" {
		t.Fatalf("err %v", err)
	}
}

func TestStreamFatalErrorMidSession(t *testing.T) {
	f := newFakeStreamServer(t, func(n int, c *fakeConn) {
		c.send(t, map[string]any{"type": "session.created", "job_id": "j"})
		c.waitFor(func() bool { return c.audioLocked() > 0 })
		c.send(t, map[string]any{"type": "error", "code": "use_batch", "message": "sent too fast"})
		time.Sleep(200 * time.Millisecond)
	})
	s, err := testClient(t, f.srv.URL).Stream(context.Background(), StreamOptions{Language: "en"})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.SendAudio(testPCM(100, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = s.Finish(ctx)
	var se *StreamError
	if !errors.As(err, &se) || se.Code != "use_batch" {
		t.Fatalf("err %v", err)
	}
}

func TestStreamRevisedFinalsAndEvents(t *testing.T) {
	f := newFakeStreamServer(t, func(n int, c *fakeConn) {
		c.send(t, map[string]any{"type": "session.created", "job_id": "j"})
		c.waitFor(c.hasFinalCommit)
		c.send(t, map[string]any{"type": "final", "index": 0, "text": "helo", "start": 0, "end": 1})
		c.send(t, map[string]any{"type": "final.revised", "index": 0, "text": "hello"})
		c.send(t, map[string]any{"type": "transcription.done", "text": "hello"})
	})
	var mu sync.Mutex
	var seen []string
	s, err := testClient(t, f.srv.URL).Stream(context.Background(), StreamOptions{
		Language: "en",
		OnEvent:  func(e StreamEvent) { mu.Lock(); seen = append(seen, e.Type); mu.Unlock() },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := s.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Finals[0].Text != "hello" || res.Finals[0].Start == nil {
		t.Fatalf("finals %+v", res.Finals)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"session.created", "final", "final.revised", "transcription.done"}
	if len(seen) != len(want) {
		t.Fatalf("events %v", seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("events %v", seen)
		}
	}
}

func TestStreamOddLengthRefused(t *testing.T) {
	s := &LiveSession{wake: make(chan struct{}, 1)}
	if err := s.SendAudio([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected an error")
	}
}
