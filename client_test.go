package aibot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestClientSubscribesAndDispatchesMessage(t *testing.T) {
	receivedSubscribe := make(chan SubscribeRequest, 1)
	receivedMessage := make(chan *Message, 1)

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade returned error: %v", err)
			return
		}
		defer conn.Close()

		var sub WsFrame[SubscribeBody]
		if err := conn.ReadJSON(&sub); err != nil {
			t.Errorf("ReadJSON subscribe returned error: %v", err)
			return
		}
		receivedSubscribe <- SubscribeRequest(sub)

		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"cmd":"aibot_msg_callback","headers":{"req_id":"req-1"},"body":{"msgtype":"text","msgid":"msg-1","text":{"content":"hello"}}}`)); err != nil {
			t.Errorf("WriteMessage returned error: %v", err)
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(Config{
		BotID:             "bot-1",
		Secret:            "secret-1",
		Endpoint:          wsURL,
		HeartbeatInterval: time.Hour,
	})
	client.OnMessage(func(ctx context.Context, msg *Message) error {
		receivedMessage <- msg
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Run(ctx)
	}()

	select {
	case sub := <-receivedSubscribe:
		if sub.Cmd != WsCmdSubscribe || sub.Body.BotID != "bot-1" || sub.Body.Secret != "secret-1" {
			t.Fatalf("subscribe = %#v, want bot credentials", sub)
		}
	case err := <-errCh:
		t.Fatalf("Run returned early: %v", err)
	case <-ctx.Done():
		t.Fatalf("timeout waiting for subscribe")
	}

	select {
	case msg := <-receivedMessage:
		if msg.Text == nil || msg.Text.Content != "hello" {
			t.Fatalf("message = %#v, want text hello", msg)
		}
	case err := <-errCh:
		t.Fatalf("Run returned early: %v", err)
	case <-ctx.Done():
		t.Fatalf("timeout waiting for message")
	}
}

func TestClientSendReplyWritesJSON(t *testing.T) {
	subscribed := make(chan struct{}, 1)
	written := make(chan json.RawMessage, 1)
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
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("ReadMessage reply returned error: %v", err)
			return
		}
		written <- append([]byte(nil), data...)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(Config{
		BotID:             "bot-1",
		Secret:            "secret-1",
		Endpoint:          wsURL,
		HeartbeatInterval: time.Hour,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = client.Run(ctx)
	}()

	select {
	case <-subscribed:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for subscribe")
	}

	if err := client.Send(ctx, NewTextReply("msg-1", "ok")); err != nil {
		t.Fatalf("Send returned error: %v", err)
	}

	select {
	case data := <-written:
		if !strings.Contains(string(data), `"aibot_respond_msg"`) {
			t.Fatalf("written = %s, want aibot_respond_msg", data)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for reply write")
	}
}

func TestClientRunReturnsWhenContextCancelled(t *testing.T) {
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

		// 保持连接空闲，模拟真实长连接等待消息时用户按 Ctrl+C。
		<-time.After(5 * time.Second)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(Config{
		BotID:             "bot-1",
		Secret:            "secret-1",
		Endpoint:          wsURL,
		HeartbeatInterval: time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Run(ctx)
	}()

	select {
	case <-subscribed:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for subscribe")
	}

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Run did not return promptly after context cancellation")
	}
}

func TestClientSendAndWaitSerializesSameReqIDUntilAck(t *testing.T) {
	subscribed := make(chan struct{}, 1)
	firstWritten := make(chan WsFrame[ReplyBody], 1)
	secondWritten := make(chan WsFrame[ReplyBody], 1)

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

		var first WsFrame[ReplyBody]
		if err := conn.ReadJSON(&first); err != nil {
			t.Errorf("ReadJSON first returned error: %v", err)
			return
		}
		firstWritten <- first

		secondRead := make(chan WsFrame[ReplyBody], 1)
		secondReadErr := make(chan error, 1)
		go func() {
			var second WsFrame[ReplyBody]
			if err := conn.ReadJSON(&second); err != nil {
				secondReadErr <- err
				return
			}
			secondRead <- second
		}()

		select {
		case second := <-secondRead:
			t.Errorf("second reply was sent before ACK: %#v", second)
			return
		case err := <-secondReadErr:
			t.Errorf("ReadJSON second before ACK returned error: %v", err)
			return
		case <-time.After(120 * time.Millisecond):
		}

		if err := conn.WriteJSON(WsFrame[struct{}]{
			Headers: WsHeaders{ReqID: first.Headers.ReqID},
			ErrCode: intPtr(0),
			ErrMsg:  "ok",
		}); err != nil {
			t.Errorf("WriteJSON ack returned error: %v", err)
			return
		}

		var second WsFrame[ReplyBody]
		select {
		case second = <-secondRead:
		case err := <-secondReadErr:
			t.Errorf("ReadJSON second returned error: %v", err)
			return
		case <-time.After(2 * time.Second):
			t.Errorf("timeout waiting for second reply")
			return
		}
		secondWritten <- second
		if err := conn.WriteJSON(WsFrame[struct{}]{
			Headers: WsHeaders{ReqID: second.Headers.ReqID},
			ErrCode: intPtr(0),
			ErrMsg:  "ok",
		}); err != nil {
			t.Errorf("WriteJSON second ack returned error: %v", err)
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

	errCh1 := make(chan error, 1)
	errCh2 := make(chan error, 1)
	go func() {
		_, err := client.SendAndWait(ctx, NewStreamReply("req-1", "stream-1", "first", false))
		errCh1 <- err
	}()

	select {
	case first := <-firstWritten:
		if first.Body.Stream == nil || first.Body.Stream.Content != "first" {
			t.Fatalf("first = %#v, want first content", first)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for first write")
	}

	go func() {
		_, err := client.SendAndWait(ctx, NewStreamReply("req-1", "stream-1", "second", true))
		errCh2 <- err
	}()

	select {
	case second := <-secondWritten:
		if second.Body.Stream == nil || second.Body.Stream.Content != "second" {
			t.Fatalf("second = %#v, want second content", second)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for second write")
	}

	if err := <-errCh1; err != nil {
		t.Fatalf("first SendAndWait error = %v", err)
	}
	if err := <-errCh2; err != nil {
		t.Fatalf("second SendAndWait error = %v", err)
	}
}

func TestClientHeartbeatAckIsHandledInternally(t *testing.T) {
	ackCalled := make(chan struct{}, 1)
	client := NewClient(Config{BotID: "bot-1", Secret: "secret-1"})
	client.OnAck(func(ctx context.Context, ack *Ack) error {
		ackCalled <- struct{}{}
		return nil
	})
	client.missedPongCount = 2

	err := client.dispatchFrame(context.Background(), []byte(`{"headers":{"req_id":"ping_1"},"errcode":0,"errmsg":"ok"}`))
	if err != nil {
		t.Fatalf("dispatchFrame returned error: %v", err)
	}
	if client.missedPongCount != 0 {
		t.Fatalf("missedPongCount = %d, want 0", client.missedPongCount)
	}
	select {
	case <-ackCalled:
		t.Fatalf("heartbeat ack should not call OnAck")
	default:
	}
}

func TestClientRunForeverReconnectsAfterConnectionClose(t *testing.T) {
	connectionCount := make(chan struct{}, 2)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade returned error: %v", err)
			return
		}
		defer conn.Close()

		_, _, _ = conn.ReadMessage()
		connectionCount <- struct{}{}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(Config{
		BotID:             "bot-1",
		Secret:            "secret-1",
		Endpoint:          wsURL,
		HeartbeatInterval: time.Hour,
		ReconnectInterval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		_ = client.RunForever(ctx)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-connectionCount:
		case <-ctx.Done():
			t.Fatalf("timeout waiting for connection %d", i+1)
		}
	}
}

func intPtr(v int) *int {
	return &v
}
