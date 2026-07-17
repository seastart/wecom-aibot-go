package aibot

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
		// 写完消息后【保持连接】直到客户端断开（测试结束 ctx 取消）。回调已改异步派发，
		// 若在此处立刻 defer Close 关连接，读循环会先读到 close 1006 让 Run 提前返回，
		// 与异步回调投递 receivedMessage 形成竞态。阻塞读一次把连接留住即可消除竞态。
		_, _, _ = conn.ReadMessage()
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

// TestClientRunForeverStopsWithErrServerDisconnected 验证：服务端下发 disconnected_event
// （新连接顶替旧连接）后，RunForever【停止重连】且返回可被 errors.Is 识别的
// ErrServerDisconnected——把「被顶替」这一真实原因暴露给调用方，而非埋在随后的 socket 关闭错误里。
func TestClientRunForeverStopsWithErrServerDisconnected(t *testing.T) {
	connections := make(chan struct{}, 4)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade returned error: %v", err)
			return
		}
		defer conn.Close()
		connections <- struct{}{}

		// 先吃掉订阅帧，再下发 disconnected_event 并关闭连接（模拟服务端「被新连接顶替」踢旧连接）。
		var sub WsFrame[SubscribeBody]
		if err := conn.ReadJSON(&sub); err != nil {
			t.Errorf("ReadJSON subscribe returned error: %v", err)
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"cmd":"aibot_event_callback","headers":{"req_id":"evt-1"},"body":{"event":{"eventtype":"disconnected_event"}}}`)); err != nil {
			t.Errorf("WriteMessage disconnected_event returned error: %v", err)
		}
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.RunForever(ctx)
	}()

	select {
	case err := <-errCh:
		// 关键断言 1：返回可被识别的哨兵错误（不是 ctx.Canceled，也不是被吞掉的裸网络错误）。
		if !errors.Is(err, ErrServerDisconnected) {
			t.Fatalf("RunForever err = %v, want errors.Is ErrServerDisconnected", err)
		}
	case <-ctx.Done():
		t.Fatalf("timeout: RunForever 未在收到 disconnected_event 后返回（可能仍在重连）")
	}

	// 关键断言 2：收到 disconnected_event 后不再发起新连接（stopReconnect 生效）。
	<-connections // 第一条连接
	select {
	case <-connections:
		t.Fatalf("收到 disconnected_event 后仍发起了重连，stopReconnect 未生效")
	case <-time.After(50 * time.Millisecond):
		// 一个重连间隔（10ms）过去仍无新连接，符合预期。
	}
}

// TestClientConnectionStateConnectedThenDisconnected 验证连接生命周期回调的两个真边沿：
// 订阅经服务端 ACK → Connected；随后服务端关连接 → Disconnected（携带断因）。
func TestClientConnectionStateConnectedThenDisconnected(t *testing.T) {
	upgrader := websocket.Upgrader{}
	release := make(chan struct{})
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
		// 回订阅 ACK（echo req_id、errcode=0）——此刻客户端才认定「真正连上」。
		if err := conn.WriteJSON(WsFrame[struct{}]{
			Headers: WsHeaders{ReqID: sub.Headers.ReqID},
			ErrCode: intPtr(0),
			ErrMsg:  "ok",
		}); err != nil {
			t.Errorf("WriteJSON subscribe ack returned error: %v", err)
			return
		}
		<-release // 收到信号后关连接，模拟掉线，触发客户端 Disconnected。
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

	type stateEvent struct {
		state ConnState
		err   error
	}
	events := make(chan stateEvent, 4)
	client.OnConnectionState(func(state ConnState, err error) {
		events <- stateEvent{state, err}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = client.Run(ctx) }()

	select {
	case ev := <-events:
		if ev.state != StateConnected {
			t.Fatalf("first event = %v, want connected", ev.state)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for connected")
	}

	close(release) // 服务端关连接
	select {
	case ev := <-events:
		if ev.state != StateDisconnected {
			t.Fatalf("second event = %v, want disconnected", ev.state)
		}
		if ev.err == nil {
			t.Fatalf("disconnected 事件应携带断因 err，实得 nil")
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for disconnected")
	}
}

// TestClientRunFailsWhenSubscribeAckTimesOut 验证「僵尸假活」被识破：socket 连上、订阅帧发出，但
// 服务端【不回订阅 ACK】——Run 必须在 ReplyAckTimeout 后带 subscribe 失败错误返回，且【绝不误报 Connected】。
// 这正是真机事故的根因场景（旧版发了订阅就当连上，永远收不到消息却一直「看着活着」）。
func TestClientRunFailsWhenSubscribeAckTimesOut(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade returned error: %v", err)
			return
		}
		defer conn.Close()
		// 读掉订阅帧但【故意不回 ACK】，保持连接开着，让客户端一直等 ACK 直到超时。
		var sub WsFrame[SubscribeBody]
		_ = conn.ReadJSON(&sub)
		<-time.After(time.Second)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(Config{
		BotID:             "bot-1",
		Secret:            "secret-1",
		Endpoint:          wsURL,
		HeartbeatInterval: time.Hour,
		ReplyAckTimeout:   100 * time.Millisecond,
	})

	connectedFired := make(chan struct{}, 1)
	client.OnConnectionState(func(state ConnState, err error) {
		if state == StateConnected {
			connectedFired <- struct{}{}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- client.Run(ctx) }()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "subscribe") {
			t.Fatalf("Run err = %v, want subscribe failure", err)
		}
	case <-ctx.Done():
		t.Fatalf("timeout: Run 未在订阅 ACK 超时后返回")
	}
	// 僵尸态从未真正连上 → 绝不能误报 Connected。
	select {
	case <-connectedFired:
		t.Fatalf("subscribe 未被 ACK 却误报了 Connected")
	default:
	}
}

// TestClientRunResetsMissedPongOnNewConnection 回归：新连接必须把 missedPongCount 归零。
//
// 事故场景（真机）：missedPongCount 挂在 Client 上跨连接复用，只在收到心跳 ACK 时清零。
// 若上一条连接因丢包把它累加到 MaxMissedPong 后断开，进程不重启、Client 复用，则新连接的
// 心跳线程首个 tick 就命中 `>= MaxMissedPong` 立即 Close，且这一刻还没发心跳、更收不到 ACK
// 来清零——每条新连接都在一个心跳间隔内被自己掐死，RunForever 永远缓不过来。
// 本测试预先把计数器污染成「超阈值」，验证建连后它已被重置为 0。
func TestClientRunResetsMissedPongOnNewConnection(t *testing.T) {
	upgrader := websocket.Upgrader{}
	release := make(chan struct{})
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
		// 回订阅 ACK → 客户端认定「真正连上」，此刻建连时的计数器重置必已执行。
		if err := conn.WriteJSON(WsFrame[struct{}]{
			Headers: WsHeaders{ReqID: sub.Headers.ReqID},
			ErrCode: intPtr(0),
			ErrMsg:  "ok",
		}); err != nil {
			t.Errorf("WriteJSON subscribe ack returned error: %v", err)
			return
		}
		<-release
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(Config{
		BotID:             "bot-1",
		Secret:            "secret-1",
		Endpoint:          wsURL,
		HeartbeatInterval: time.Hour, // 心跳不触发，隔离出「建连重置」这一行为
		ReplyAckTimeout:   time.Second,
	})

	// 污染计数器：模拟上一条连接遗留的「已达阈值」状态。必须在 Run 之前设置。
	client.missedPongCount = client.cfg.MaxMissedPong + 5

	connected := make(chan struct{}, 1)
	client.OnConnectionState(func(state ConnState, _ error) {
		if state == StateConnected {
			connected <- struct{}{}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = client.Run(ctx) }()

	select {
	case <-connected:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for connected")
	}

	client.mu.Lock()
	got := client.missedPongCount
	client.mu.Unlock()
	if got != 0 {
		t.Fatalf("建连后 missedPongCount = %d, want 0（新连接未重置遗留计数器，会在首个心跳 tick 自我掐死）", got)
	}
	close(release)
}

func intPtr(v int) *int {
	return &v
}

// attemptCaptureHandler 是一个极简 slog.Handler，只从「wecom 等待重连」日志里抓取 attempt 值，
// 供测试观察退避计数的演进（库未把 attempt 暴露为字段，只能经日志外露）。
type attemptCaptureHandler struct {
	mu       *sync.Mutex
	attempts *[]int64
}

func (h attemptCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h attemptCaptureHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h attemptCaptureHandler) WithGroup(string) slog.Handler            { return h }
func (h attemptCaptureHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message != "wecom 等待重连" {
		return nil
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "attempt" {
			h.mu.Lock()
			*h.attempts = append(*h.attempts, a.Value.Int64())
			h.mu.Unlock()
			return false
		}
		return true
	})
	return nil
}

// TestClientRunForeverResetsBackoffAfterSuccessfulConnect 回归：一次成功连上后，指数退避计数必须归零。
//
// 缺陷场景：attempt 在 RunForever 里单调递增、成功连上从不重置——它把「连续失败次数」错当成
// 「进程生命周期内的总断开次数」。于是健康连接偶发抖动也会被历史累积的 attempt 拖到 30s 上限，
// 越连越慢、最终永久钉死在封顶。本测试让服务端「失败两次 → 成功连上一次 → 再失败」，
// 断言成功连上后那次的 attempt 回落到 1（而非继续递增到 3）。
func TestClientRunForeverResetsBackoffAfterSuccessfulConnect(t *testing.T) {
	var connIdx int64
	var idxMu sync.Mutex

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		idxMu.Lock()
		connIdx++
		n := connIdx
		idxMu.Unlock()

		// 第 3 条连接：回订阅 ACK → 客户端认定「真正连上」（connectedThisRun=true），随后关连接。
		// 其余连接：读掉订阅帧后直接关，不回 ACK → Run 在订阅前就断，从未连上。
		var sub WsFrame[SubscribeBody]
		_ = conn.ReadJSON(&sub)
		if n == 3 {
			_ = conn.WriteJSON(WsFrame[struct{}]{
				Headers: WsHeaders{ReqID: sub.Headers.ReqID},
				ErrCode: intPtr(0),
				ErrMsg:  "ok",
			})
			time.Sleep(30 * time.Millisecond) // 留出时间让客户端读到 ACK、触发 Connected 边沿
		}
	}))
	defer server.Close()

	var mu sync.Mutex
	var attempts []int64
	logger := slog.New(attemptCaptureHandler{mu: &mu, attempts: &attempts})

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(Config{
		BotID:             "bot-1",
		Secret:            "secret-1",
		Endpoint:          wsURL,
		HeartbeatInterval: time.Hour,
		ReconnectInterval: time.Millisecond, // 退避基数压到 1ms，测试无需真等 3s/6s
		ReplyAckTimeout:   200 * time.Millisecond,
		Logger:            logger,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = client.RunForever(ctx) }()

	// 等到至少 4 条「等待重连」记录（覆盖 失败#1、失败#2、成功后#3、再失败#4）。
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n := len(attempts)
		mu.Unlock()
		if n >= 4 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout：只收集到 %d 条重连记录，不足 4 条", n)
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()

	mu.Lock()
	got := append([]int64(nil), attempts...)
	mu.Unlock()

	// 关键断言：第 3 条连接成功连上，故其后那次重连的 attempt 必须回落到 1。
	// 若退避未重置，序列会是 1,2,3,4...（第 3 条会是 3）。
	if got[0] != 1 || got[1] != 2 {
		t.Fatalf("成功连上前的退避序列 = %v，期望以 1,2 递增", got[:2])
	}
	if got[2] != 1 {
		t.Fatalf("成功连上后 attempt = %d，期望重置为 1（退避未在成功连接后归零，会越连越慢直至钉死 30s 上限）", got[2])
	}
}
