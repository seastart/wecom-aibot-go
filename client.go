package aibot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const defaultWebSocketEndpoint = "wss://openws.work.weixin.qq.com"

// Config controls one WeCom intelligent robot long connection.
type Config struct {
	// BotID is the intelligent robot BotID from WeCom admin console.
	BotID string
	// Secret is the long-connection secret. Keep it out of source control.
	Secret string
	// Endpoint defaults to the official WeCom long-connection endpoint.
	Endpoint string
	// Header allows callers to pass proxy/auth headers if their runtime needs it.
	Header http.Header
	// HeartbeatInterval controls client-side keepalive. Defaults to 30 seconds.
	HeartbeatInterval time.Duration
	// ReconnectInterval is reserved for callers that wrap Run in their own loop.
	ReconnectInterval time.Duration
	// ReplyAckTimeout bounds how long SendAndWait waits for a server ACK.
	ReplyAckTimeout time.Duration
	// MaxMissedPong closes the connection after this many missed heartbeat ACKs.
	MaxMissedPong int
}

// MessageHandler handles normalized incoming messages.
// 回调在独立 goroutine 中执行（见 dispatchFrame），返回的 error 仅供调用方
// 自行处理，库不再据此中断连接。回调内可安全调用 SendAndWait 等待 ack。
type MessageHandler func(ctx context.Context, msg *Message) error

// EventHandler handles incoming events such as disconnect notifications.
// 同 MessageHandler：异步执行，返回的 error 不会中断连接。
type EventHandler func(ctx context.Context, event *Event) error

// AckHandler handles server acknowledgements for subscribe/reply/push requests.
// 仅当 ack 未被 SendAndWait 认领时才回调；异步执行，返回的 error 不会中断连接。
type AckHandler func(ctx context.Context, ack *Ack) error

// Client owns exactly one WebSocket connection for exactly one robot.
// WeCom only allows one active long connection per robot, so the library keeps
// this type intentionally single-connection instead of hiding a connection pool.
type Client struct {
	cfg Config

	mu              sync.RWMutex
	conn            *websocket.Conn
	pendingAcks     map[string]chan Ack
	replyQueues     map[string]chan *queuedReply
	missedPongCount int
	stopReconnect   bool

	writeMu sync.Mutex

	onMessage MessageHandler
	onEvent   EventHandler
	onAck     AckHandler
}

type queuedReply struct {
	ctx     context.Context
	payload any
	result  chan sendAndWaitResult
}

type sendAndWaitResult struct {
	ack *Ack
	err error
}

// NewClient creates a long-connection client.
func NewClient(cfg Config) *Client {
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultWebSocketEndpoint
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.ReconnectInterval <= 0 {
		cfg.ReconnectInterval = 3 * time.Second
	}
	if cfg.ReplyAckTimeout <= 0 {
		cfg.ReplyAckTimeout = 5 * time.Second
	}
	if cfg.MaxMissedPong <= 0 {
		cfg.MaxMissedPong = 3
	}
	return &Client{
		cfg:         cfg,
		pendingAcks: make(map[string]chan Ack),
		replyQueues: make(map[string]chan *queuedReply),
	}
}

// OnMessage registers the business message callback.
func (c *Client) OnMessage(handler MessageHandler) {
	c.onMessage = handler
}

// OnEvent registers the event callback.
func (c *Client) OnEvent(handler EventHandler) {
	c.onEvent = handler
}

// OnAck registers the acknowledgement callback.
func (c *Client) OnAck(handler AckHandler) {
	c.onAck = handler
}

// ErrServerDisconnected 表示 RunForever 因服务端主动下发 disconnected_event 而停止重连。
//
// 第一性原理：企微限制「同一机器人同一时刻只能有一条有效长连接」，当有【新连接】用同一 BotID
// 建立时，服务端会向【旧连接】推送 disconnected_event 并断开它（见 message.go 的
// EventTypeDisconnected 说明）。这属于「被顶替」，库据此关闭自动重连——否则旧连接会不停重连、
// 与新连接互相踢下线。
//
// 用途：RunForever 返回的错误可用 errors.Is(err, ErrServerDisconnected) 判定。为 true 时应理解为
// 「别处已用同一 BotID 建了连接」，通常【不应】立即重连；为 false 时（RunForever 只在 ctx 取消时
// 才有其它非重连返回）则是正常关停。底层那条 "use of closed network connection" 只是断开后的
// 表象错误，会作为 %v 附在本哨兵错误之后，仅供排查参考。
var ErrServerDisconnected = errors.New("wecom aibot: server pushed disconnected_event (a new connection took over this bot)")

// RunForever keeps the long connection alive until ctx is cancelled.
//
// 它在一次 Run 断开后按指数退避自动重连；仅在两种情况退出：ctx 取消（返回 ctx.Err()），
// 或服务端下发 disconnected_event 令其停止重连（返回被 ErrServerDisconnected 包裹的错误，
// 详见该哨兵错误说明）。
func (c *Client) RunForever(ctx context.Context) error {
	attempt := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := c.Run(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		c.mu.RLock()
		stopReconnect := c.stopReconnect
		c.mu.RUnlock()
		if stopReconnect {
			// 走到这里【只有一种可能】：服务端下发了 disconnected_event，
			// handleReconnectControlEvent 把 stopReconnect 置了 true（全库仅此一处置位）。
			// 此时 err 通常是随后 socket 被关导致的 "use of closed network connection"——
			// 那只是【表象】，真正原因是「本连接被新连接顶替」。故在这里把真实原因用哨兵
			// 错误 ErrServerDisconnected 显式包裹暴露给调用方，让其能 errors.Is 区分
			// 「被顶替（不该盲目重连去互相踢）」与普通故障，而不必去猜那条网络错误的含义。
			if err != nil {
				return fmt.Errorf("%w: %v", ErrServerDisconnected, err)
			}
			return ErrServerDisconnected
		}

		attempt++
		delay := c.reconnectDelay(attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Run connects, subscribes, starts heartbeat, and dispatches incoming frames.
func (c *Client) Run(ctx context.Context) error {
	if c.cfg.BotID == "" || c.cfg.Secret == "" {
		return errors.New("wecom aibot: BotID and Secret are required")
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.cfg.Endpoint, c.cfg.Header)
	if err != nil {
		return err
	}
	defer conn.Close()

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	ctxDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// ReadMessage 会一直阻塞等待网络帧。Ctrl+C 只会取消 context，
			// 不会自动打断底层 socket 读，所以这里主动关闭连接让读循环退出。
			_ = conn.Close()
		case <-ctxDone:
		}
	}()
	defer func() {
		close(ctxDone)
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
	}()

	// 订阅是长连接握手后的第一条业务消息；只有订阅成功后，
	// 企业微信才会把该机器人对应的消息和事件推到这条连接上。
	if err := c.Send(ctx, NewSubscribeRequest(c.cfg.BotID, c.cfg.Secret)); err != nil {
		return err
	}

	heartbeatDone := make(chan struct{})
	go c.heartbeatLoop(ctx, heartbeatDone)
	defer close(heartbeatDone)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		if err := c.dispatchFrame(ctx, data); err != nil {
			return err
		}
	}
}

// SendAndWait sends one frame and waits for the server ACK with the same req_id.
func (c *Client) SendAndWait(ctx context.Context, payload interface{ GetHeaders() WsHeaders }) (*Ack, error) {
	reqID := payload.GetHeaders().ReqID
	if reqID == "" {
		return nil, errors.New("wecom aibot: payload headers.req_id is required")
	}

	item := &queuedReply{
		ctx:     ctx,
		payload: payload,
		result:  make(chan sendAndWaitResult, 1),
	}
	queue := c.replyQueue(reqID)

	select {
	case queue <- item:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case result := <-item.result:
		return result.ack, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Send writes one protocol payload to the active WebSocket connection.
func (c *Client) Send(ctx context.Context, payload any) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return errors.New("wecom aibot: websocket is not connected")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		// gorilla/websocket allows one concurrent reader and one concurrent
		// writer. 所有业务回复、主动推送和心跳都走同一把写锁，避免并发写帧
		// 导致连接状态损坏。
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		done <- conn.WriteMessage(websocket.TextMessage, data)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (c *Client) replyQueue(reqID string) chan *queuedReply {
	c.mu.Lock()
	defer c.mu.Unlock()

	queue := c.replyQueues[reqID]
	if queue == nil {
		queue = make(chan *queuedReply, 32)
		c.replyQueues[reqID] = queue
		go c.processReplyQueue(reqID, queue)
	}
	return queue
}

func (c *Client) processReplyQueue(reqID string, queue <-chan *queuedReply) {
	for item := range queue {
		ackCh := make(chan Ack, 1)
		c.mu.Lock()
		c.pendingAcks[reqID] = ackCh
		c.mu.Unlock()

		err := c.Send(item.ctx, item.payload)
		if err != nil {
			c.clearPendingAck(reqID)
			item.result <- sendAndWaitResult{err: err}
			continue
		}

		select {
		case ack := <-ackCh:
			if ack.ErrCode != 0 {
				item.result <- sendAndWaitResult{ack: &ack, err: AckError{Ack: ack}}
			} else {
				item.result <- sendAndWaitResult{ack: &ack}
			}
		case <-time.After(c.cfg.ReplyAckTimeout):
			c.clearPendingAck(reqID)
			item.result <- sendAndWaitResult{err: ErrAckTimeout{ReqID: reqID, Timeout: c.cfg.ReplyAckTimeout}}
		case <-item.ctx.Done():
			c.clearPendingAck(reqID)
			item.result <- sendAndWaitResult{err: item.ctx.Err()}
		}
	}
}

func (c *Client) clearPendingAck(reqID string) {
	c.mu.Lock()
	delete(c.pendingAcks, reqID)
	c.mu.Unlock()
}

func (c *Client) heartbeatLoop(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			c.mu.Lock()
			if c.missedPongCount >= c.cfg.MaxMissedPong {
				conn := c.conn
				c.mu.Unlock()
				if conn != nil {
					// 连续心跳 ACK 缺失说明连接已经不可信。主动关闭连接，
					// 让读循环退出，并交给 RunForever 或调用方做重连。
					_ = conn.Close()
				}
				return
			}
			c.missedPongCount++
			c.mu.Unlock()

			_ = c.Send(ctx, NewHeartbeatRequest())
		}
	}
}

func (c *Client) dispatchFrame(ctx context.Context, data []byte) error {
	frame, err := ParseFrame(data)
	if err != nil {
		return err
	}

	switch frame.Kind {
	case FrameKindMessage:
		// 业务回调必须【异步】执行。读循环是唯一能 conn.ReadMessage 的 goroutine，
		// 服务端 ack 也只能由它读出来后经 resolvePendingAck 投递。一旦回调里
		// 同步调用 SendAndWait 等待 ack，读循环就会卡在回调内、永远读不到那条
		// ack，直到 5s 超时——表现为「企微不回 ack」的假失败，实为自我死锁。
		// 另起 goroutine 后读循环始终空闲，回调内的 SendAndWait 才能拿到 ack。
		// 代价：回调并发执行、不保证先后顺序，回调返回的 error 不再中断连接
		//（业务错误请在回调内部自行处理/记录）。
		if c.onMessage != nil {
			go c.onMessage(ctx, frame.Message)
		}
	case FrameKindEvent:
		// 断开控制事件决定是否停止自动重连，必须在读循环内同步处理。
		c.handleReconnectControlEvent(frame.Event)
		// 业务事件回调异步派发，理由同 onMessage。
		if c.onEvent != nil {
			go c.onEvent(ctx, frame.Event)
		}
	case FrameKindAck:
		// ack 的认领必须【同步】完成：正阻塞在 SendAndWait 的调用方等着
		// resolvePendingAck 把结果投递回去，不能延后。
		if c.handleHeartbeatAck(frame.Ack) {
			return nil
		}
		if c.resolvePendingAck(frame.Ack) {
			return nil
		}
		// 未被认领的 ack 才交给用户回调，异步执行避免阻塞读循环。
		if c.onAck != nil {
			go c.onAck(ctx, frame.Ack)
		}
	}
	return nil
}

func (c *Client) reconnectDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := c.cfg.ReconnectInterval
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= 30*time.Second {
			return 30 * time.Second
		}
	}
	return delay
}

func (c *Client) handleReconnectControlEvent(event *Event) {
	if event == nil || event.Event == nil || event.Event.EventType != EventTypeDisconnected {
		return
	}
	c.mu.Lock()
	c.stopReconnect = true
	c.mu.Unlock()
}

func (c *Client) handleHeartbeatAck(ack *Ack) bool {
	if ack == nil || !strings.HasPrefix(ack.ReqID, WsCmdHeartbeat) {
		return false
	}
	if ack.ErrCode == 0 {
		c.mu.Lock()
		c.missedPongCount = 0
		c.mu.Unlock()
	}
	return true
}

func (c *Client) resolvePendingAck(ack *Ack) bool {
	if ack == nil || ack.ReqID == "" {
		return false
	}

	c.mu.Lock()
	ch := c.pendingAcks[ack.ReqID]
	if ch != nil {
		delete(c.pendingAcks, ack.ReqID)
	}
	c.mu.Unlock()

	if ch == nil {
		return false
	}
	ch <- *ack
	return true
}
