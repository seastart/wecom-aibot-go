package aibot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	// Logger 注入调用方的结构化日志器；为 nil 时库【完全静默】（内部回退到 slog.DiscardHandler）。
	//
	// 第一性原理：库是纯机制层，「打不打日志、打到哪、什么级别」是调用方的策略——库不应擅自写
	// 全局 slog.Default() 去污染每个使用方（本库被 vinez 与 zm-deploy 共用）。调用方需要可观测性
	// 时注入自己的 logger，不需要时零配置即零噪音，两个消费方互不影响。
	Logger *slog.Logger
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

// ConnState 是长连接的生命周期【边沿态】——只在「真正连上」与「掉线」两个跃迁点各触发一次。
//
// 第一性原理：断线重连是高频内部事件（拨号失败/退避/重试可能每几十秒一轮），若把每次重试都
// 抛给调用方会刷屏且无信息量。调用方真正关心的只是「现在到底能不能收发」这一【状态跃迁】，
// 故只保留两个边沿态、中间的重连过程仅落库内 DEBUG 日志。
type ConnState int

const (
	// StateDisconnected 长连接掉线（曾经连上、现在断了）。回调 err 携带断因。
	StateDisconnected ConnState = iota
	// StateConnected 长连接已建立【且订阅经服务端 ACK 确认】——此刻才真正能收消息。
	StateConnected
)

// String 返回边沿态的可读名，便于日志。
func (s ConnState) String() string {
	switch s {
	case StateConnected:
		return "connected"
	case StateDisconnected:
		return "disconnected"
	default:
		return fmt.Sprintf("ConnState(%d)", int(s))
	}
}

// ConnStateHandler 连接生命周期回调；err 仅在 StateDisconnected 时有意义（携带断因）。
//
// 契约：回调【同步】在库的运行 goroutine 上调用（边沿态低频，无需异步），故实现【必须尽快返回、
// 不得阻塞】——阻塞会拖住重连。典型用法是记一行日志 / 更新一个原子状态，不要在里面做 I/O 等待。
type ConnStateHandler func(state ConnState, err error)

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
	// connected 记录「上一次向调用方广播的连接状态」，用于把重连过程去抖成 Connected/Disconnected
	// 两个真边沿：仅当此值发生翻转时才触发回调（见 transitionConn），避免退避重试期间反复误报。
	connected bool
	// connectedThisRun 标记「本轮 Run 是否成功连上过（订阅 ACK 通过）」。RunForever 每轮 Run 前清零，
	// transitionConn(true) 置位；据此判断本次断开前连接是否真正建立过，从而决定退避计数是重置还是累加。
	// 第一性原理：指数退避衡量的应是「连续失败次数」，一旦成功连上就该清零——否则健康连接偶尔抖一下也
	// 会被历史累积的 attempt 拖到 30s 上限，越连越慢、进程生命周期内永久钉死在封顶。
	connectedThisRun bool

	writeMu sync.Mutex

	onMessage   MessageHandler
	onEvent     EventHandler
	onAck       AckHandler
	onConnState ConnStateHandler
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
	if cfg.Logger == nil {
		// 一次性把 nil 解析成「丢弃日志器」，运行期各处直接用 cfg.Logger、无需处处判空。
		cfg.Logger = slog.New(slog.DiscardHandler)
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

// OnConnectionState 注册连接生命周期回调（见 ConnStateHandler 契约）。应在 Run/RunForever 之前调用。
func (c *Client) OnConnectionState(handler ConnStateHandler) {
	c.onConnState = handler
}

// transitionConn 把「原始的连上/断开事件」去抖成【真边沿】后广播给调用方并落库内日志。
//
// 第一性原理：只有当状态相对上一次广播【真的翻转】时才通知——这样退避重连期间连续多次拨号失败
// 只会在「首次从 connected 掉下来」时报一次 Disconnected，之后保持静默，直到真的重新连上才报
// Connected。首次启动就连不上（从未 connected）时不会误报 Disconnected（由后续看门狗负责，不在本层）。
func (c *Client) transitionConn(connected bool, cause error) {
	c.mu.Lock()
	changed := c.connected != connected
	c.connected = connected
	if connected {
		// 记录本轮 Run 曾成功连上，供 RunForever 重置退避计数（不受掉线时置回 false 影响）。
		c.connectedThisRun = true
	}
	c.mu.Unlock()
	if !changed {
		return
	}
	if connected {
		c.cfg.Logger.Info("wecom 长连接已建立并订阅成功")
	} else {
		c.cfg.Logger.Warn("wecom 长连接断开", "err", cause)
	}
	if c.onConnState != nil {
		state := StateDisconnected
		if connected {
			state = StateConnected
		}
		c.onConnState(state, cause)
	}
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

		c.mu.Lock()
		c.connectedThisRun = false
		c.mu.Unlock()

		err := c.Run(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// 掉线的 Disconnected 边沿已由 Run 的 defer 广播（Run 独占单条连接的完整生命周期），此处不再重复。
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
			c.cfg.Logger.Error("wecom 长连接被同 BotID 的新连接顶替，停止重连", "err", err)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrServerDisconnected, err)
			}
			return ErrServerDisconnected
		}

		// 本轮曾成功连上过 → 这是一次「健康连接的偶发掉线」，退避计数清零，从最小间隔重新退避；
		// 从未连上（连拨号/订阅都没过）→ 累加 attempt，指数退避避免猛捶不可用的服务端。
		c.mu.RLock()
		connectedThisRun := c.connectedThisRun
		c.mu.RUnlock()
		if connectedThisRun {
			attempt = 0
		}

		attempt++
		delay := c.reconnectDelay(attempt)
		// 重连过程只落 DEBUG（高频、无状态跃迁），避免刷屏；真正的边沿由 transitionConn 报。
		c.cfg.Logger.Debug("wecom 等待重连", "attempt", attempt, "delay", delay.String())
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Run connects, subscribes (awaiting the server ACK), starts heartbeat, and dispatches incoming frames.
//
// 与旧版的关键差异（订阅要等 ACK）：订阅从「发了就不管」升级为【等服务端 ACK 且 errcode==0】才算连上——
// 这样才能识破「socket 连上、订阅帧发了、心跳也通，但服务端没真正把订阅绑上」的僵尸假活（真机事故根因）。
// 为此读循环必须【先于】等 ACK 启动：ACK 只能由读循环读出并经 resolvePendingAck 投递，若在读循环起来前
// 同步等 ACK 会自锁死锁（同 dispatchFrame 注释所述）。
func (c *Client) Run(ctx context.Context) (retErr error) {
	if c.cfg.BotID == "" || c.cfg.Secret == "" {
		return errors.New("wecom aibot: BotID and Secret are required")
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.cfg.Endpoint, c.cfg.Header)
	if err != nil {
		return err
	}
	defer conn.Close()

	// 一条连接的【完整生命周期】都由 Run 拥有：连上（订阅 ACK 确认）广播 Connected，退出时广播
	// Disconnected（带断因）。放在 defer 里统一覆盖所有返回路径；transitionConn 的边沿去抖保证
	// 「从未连上就失败」的路径不会误报 Disconnected。ctx 取消属【优雅关停】、非故障，不广播。
	defer func() {
		if ctx.Err() == nil {
			c.transitionConn(false, retErr)
		}
	}()

	c.mu.Lock()
	c.conn = conn
	// missedPongCount 是「本条连接」的心跳健康度，必须随新连接归零。
	// 第一性原理：它挂在 Client 上跨连接复用，而只在收到心跳 ACK 时才清零；
	// 若上一条连接把它累加到 MaxMissedPong 后断开（如网络抖动/代理丢包），
	// 新连接的心跳线程首个 tick 就会命中 `>= MaxMissedPong` 而立即 Close，
	// 且这一刻还没来得及发心跳、更收不到 ACK 来清零 —— 于是每条新连接都在
	// ~1 个心跳间隔内被自己掐死，RunForever 永远重连不上、进程不重启就出不来。
	// 在建连处清零，保证每条连接都从「健康」起步。
	c.missedPongCount = 0
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

	heartbeatDone := make(chan struct{})
	go c.heartbeatLoop(ctx, heartbeatDone)
	defer close(heartbeatDone)

	// 读循环先起（独立 goroutine），成为这条连接【唯一】的 reader；它读到的 ack 经 resolvePendingAck
	// 投递给正在等待的 SendAndWait（含下面的订阅）。终止错误经 readErr 回传（缓冲 1，Run 提前返回也不泄漏）。
	readErr := make(chan error, 1)
	go func() {
		readErr <- c.readLoop(ctx, conn)
	}()

	// 订阅并【等服务端 ACK】。超时复用 ReplyAckTimeout（与其它 SendAndWait 同语义：等一次服务端应答）。
	subCtx, subCancel := context.WithTimeout(ctx, c.cfg.ReplyAckTimeout)
	defer subCancel()
	subResult := make(chan error, 1)
	go func() {
		_, err := c.SendAndWait(subCtx, NewSubscribeRequest(c.cfg.BotID, c.cfg.Secret))
		subResult <- err
	}()

	// 同时等「订阅结果」与「读循环终止」两者之一：
	//   - 订阅先回：errcode!=0/超时 → 当作连接不可用返回（RunForever 会重连）；成功 → 广播 Connected。
	//   - 读循环先终止：多半是【订阅 ACK 还没来连接就被顶替/断开】——直接按断开处理，否则会傻等订阅
	//     ACK 到 subCtx 超时，把「被顶替」误报成超时（顶替检测测试正覆盖此路径）。
	select {
	case err := <-subResult:
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("wecom aibot: subscribe failed: %w", err)
		}
	case err := <-readErr:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return err
		}
		return errors.New("wecom aibot: connection closed before subscribe ack")
	}

	// 订阅经服务端确认 → 连接【真正可用】，广播 Connected 边沿。
	c.transitionConn(true, nil)

	// 稳态：阻塞直到读循环因网络错/心跳失联/ctx 取消而终止。
	err = <-readErr
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// readLoop 是这条连接的唯一读者：循环读帧并派发，直到出错或 ctx 取消。抽成独立函数，是为了让订阅
// 能在它运行期间等到服务端 ACK（见 Run 的说明），避免「等 ACK 的 goroutine 与读 ACK 的 goroutine
// 同为一个」造成的自锁死锁。
func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn) error {
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
