package aibot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestUploadMediaUsesInitChunkFinishProtocol(t *testing.T) {
	var cmds []string
	subscribed := make(chan struct{}, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade returned error: %v", err)
			return
		}
		defer conn.Close()

		_, _, _ = conn.ReadMessage()
		subscribed <- struct{}{}
		for {
			var frame WsFrame[json.RawMessage]
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			cmds = append(cmds, frame.Cmd)

			body := json.RawMessage(`{}`)
			if frame.Cmd == WsCmdUploadMediaInit {
				body = json.RawMessage(`{"upload_id":"upload-1"}`)
			}
			if frame.Cmd == WsCmdUploadMediaFinish {
				body = json.RawMessage(`{"type":"image","media_id":"media-1","created_at":"now"}`)
			}
			_ = conn.WriteJSON(WsFrame[json.RawMessage]{
				Headers: frame.Headers,
				ErrCode: intPtr(0),
				ErrMsg:  "ok",
				Body:    body,
			})
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(Config{
		BotID:             "bot-1",
		Secret:            "secret-1",
		Endpoint:          wsURL,
		HeartbeatInterval: time.Hour,
		ReplyAckTimeout:   time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		_ = client.Run(ctx)
	}()

	select {
	case <-subscribed:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for subscribe")
	}

	result, err := client.UploadMedia(ctx, []byte("hello"), UploadMediaOptions{Type: MessageTypeImage, Filename: "a.png"})
	if err != nil {
		t.Fatalf("UploadMedia returned error: %v", err)
	}
	if result.MediaID != "media-1" {
		t.Fatalf("MediaID = %q, want media-1", result.MediaID)
	}
	want := []string{WsCmdUploadMediaInit, WsCmdUploadMediaChunk, WsCmdUploadMediaFinish}
	if len(cmds) != len(want) {
		t.Fatalf("cmds = %#v, want %#v", cmds, want)
	}
	for i := range want {
		if cmds[i] != want[i] {
			t.Fatalf("cmds = %#v, want %#v", cmds, want)
		}
	}
}
