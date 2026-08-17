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
				// created_at 按【真机实际形态】返回裸数字：官方文档的响应示例把它写成带引号的
				// 字符串，此前这里照文档写 mock，于是把「库声明成 string、真机返回数字」的解析
				// 崩溃整整掩盖了过去（真机事故 2026-08-17）。mock 要照着服务端写，不是照着文档写。
				body = json.RawMessage(`{"type":"image","media_id":"media-1","created_at":1380000000}`)
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
	if result.CreatedAt != 1380000000 {
		t.Fatalf("CreatedAt = %d, want 1380000000", result.CreatedAt)
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
