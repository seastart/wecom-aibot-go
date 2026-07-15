package aibot

// SubscribeBody is the body of an aibot_subscribe frame.
type SubscribeBody struct {
	BotID  string `json:"bot_id"`
	Secret string `json:"secret"`
}

// SubscribeRequest is sent immediately after the WebSocket handshake.
type SubscribeRequest = WsFrame[SubscribeBody]

// NewSubscribeRequest builds the aibot_subscribe request required by WeCom.
func NewSubscribeRequest(botID, secret string) SubscribeRequest {
	return SubscribeRequest{
		Cmd:     WsCmdSubscribe,
		Headers: WsHeaders{ReqID: NewReqID(WsCmdSubscribe)},
		Body:    SubscribeBody{BotID: botID, Secret: secret},
	}
}

// TextMessageBody is reused by reply and push payloads.
type TextMessageBody struct {
	Content string `json:"content"`
}

// MarkdownMessageBody is reused by reply and push payloads.
type MarkdownMessageBody struct {
	Content string `json:"content"`
	// Feedback 可选反馈信息，非空时该消息带反馈按钮（主动回复的 markdown 支持）。见 WithFeedback。
	Feedback *Feedback `json:"feedback,omitempty"`
}

// ReplyBody replies to a message received from the robot long connection.
type ReplyBody struct {
	MsgType      MessageType          `json:"msgtype"`
	Text         *TextMessageBody     `json:"text,omitempty"`
	Markdown     *MarkdownMessageBody `json:"markdown,omitempty"`
	Stream       *StreamMessageBody   `json:"stream,omitempty"`
	TemplateCard TemplateCard         `json:"template_card,omitempty"`
	ResponseType string               `json:"response_type,omitempty"`
	UserIDs      []string             `json:"userids,omitempty"`
	File         *MediaMessageBody    `json:"file,omitempty"`
	Image        *MediaMessageBody    `json:"image,omitempty"`
	Voice        *MediaMessageBody    `json:"voice,omitempty"`
	Video        *VideoMessageBody    `json:"video,omitempty"`
}

// ReplyMessage is the aibot_respond_msg frame.
type ReplyMessage = WsFrame[ReplyBody]

// TemplateCard is intentionally a loose map because WeCom template cards have
// many optional layouts. Callers can pass the official JSON shape directly.
type TemplateCard map[string]any

// Feedback 是一条回复的反馈信息（对应官方各回复场景里的 feedback 对象）。
//
// 第一性原理：feedback 不是某个 msgtype 的专属字段，而是「可附着在一条回复上」的横切属性——
// stream / template_card / stream_with_template_card / update_template_card 四种回复场景都支持，
// 分别挂在 stream.feedback 与/或 template_card.feedback 下。故这里建成一个共享类型，
// 用包级 WithFeedback 统一挂载，而非给每个 msgtype 造一个 XxxWithFeedback 构造函数。
//
// 设置后企微会给这条回复渲染「准确/不准确」按钮，用户点击触发 feedback_event 回调
// （详情见 FeedbackEvent），回调的 feedback_event.id 会原样带回这里设置的 ID，可用于复盘回复效果。
type Feedback struct {
	// ID 反馈 id，回调 feedback_event.id 原样带回。有效长度 256 字节以内，须 utf-8。
	// 约束（官方）：只在「流式消息首次回复」时设置有效——多帧流式回复应挂在首帧上。
	ID string `json:"id"`
}

// StreamMessageBody is the body of a stream reply.
type StreamMessageBody struct {
	ID      string `json:"id"`
	Finish  bool   `json:"finish,omitempty"`
	Content string `json:"content,omitempty"`
	// Feedback 可选反馈信息，非空时该回复带反馈按钮。一般用 WithFeedback 挂载，见其说明。
	Feedback *Feedback `json:"feedback,omitempty"`
}

// NewStreamReply creates a stream reply. requestID must come from
// the callback frame headers.req_id, not from body.msgid.
func NewStreamReply(requestID, streamID, content string, finish bool) ReplyMessage {
	return ReplyMessage{
		Cmd:     WsCmdRespondMessage,
		Headers: WsHeaders{ReqID: requestID},
		Body: ReplyBody{
			MsgType: MessageTypeStream,
			Stream: &StreamMessageBody{
				ID:      streamID,
				Finish:  finish,
				Content: content,
			},
		},
	}
}

// feedbackBody 由 ReplyBody / PushBody 的指针实现（见各自 attachFeedback），
// 让 WithFeedback 用同一个入口同时覆盖【被动回复】(aibot_respond_msg) 与【主动回复/推送】
// (aibot_send_msg)——反馈是横切属性，两种下发路径都支持，不该各写一份。
type feedbackBody interface {
	attachFeedback(fb *Feedback)
}

// attachFeedback 把反馈挂到 body 内所有支持反馈的子消息上。官方支持反馈的 body 为
// stream / markdown / template_card；这里对存在的接入点逐一挂载，其余忽略。
func (b *ReplyBody) attachFeedback(fb *Feedback) {
	if b.Stream != nil {
		b.Stream.Feedback = fb
	}
	if b.Markdown != nil {
		b.Markdown.Feedback = fb
	}
	if b.TemplateCard != nil {
		// TemplateCard 是 loose map（官方卡片布局多变），按 map 键写入，marshal 出 {"feedback":{"id":...}}。
		b.TemplateCard["feedback"] = *fb
	}
}

func (b *PushBody) attachFeedback(fb *Feedback) {
	if b.Markdown != nil {
		b.Markdown.Feedback = fb
	}
	if b.TemplateCard != nil {
		b.TemplateCard["feedback"] = *fb
	}
}

// WithFeedback 给一条下发消息挂上反馈 id，使其渲染「准确/不准确」按钮，用户反馈时触发 feedback_event
// 回调（feedback_event.id 原样带回此 id）。id 为空则原样返回（不挂反馈）。
//
// 第一性原理：反馈不属于某个 msgtype，也不分被动/主动——它是「附着在一条消息上」的横切属性。
// 故本函数用泛型统一接收【被动回复帧】ReplyMessage 与【主动推送帧】PushMessage，把 feedback 挂到
// 该帧实际携带的子 body 上（stream / markdown / template_card），无需为每种消息类型各造一个构造函数。
//
// 官方约束：反馈只在「流式消息首次回复」时设置有效，故多帧流式回复请在首帧
// （finish=false 的占位帧）调用本函数，后续覆盖帧无需再挂。
//
// 典型用法：
//
//	aibot.WithFeedback(aibot.NewStreamReply(reqID, sid, "vinezing...", false), feedbackID) // 被动流式回复
//	aibot.WithFeedback(aibot.NewMarkdownPush(chatID, ct, "**完成**"), feedbackID)          // 主动 markdown 推送
func WithFeedback[B any, PB interface {
	*B
	feedbackBody
}](frame WsFrame[B], id string) WsFrame[B] {
	if id != "" {
		PB(&frame.Body).attachFeedback(&Feedback{ID: id})
	}
	return frame
}

// NewTextReply creates a one-shot stream reply for ordinary text content.
// 企业微信长连接普通回复使用 msgtype=stream；直接回复 msgtype=text 会返回
// 40008 invalid message type。requestID 必须来自回调帧 headers.req_id。
func NewTextReply(requestID, content string) ReplyMessage {
	return NewStreamReply(requestID, NewReqID("stream"), content, true)
}

// NewMarkdownReply creates a one-shot stream reply for markdown-capable content.
// Long-connection stream content supports Markdown formatting.
func NewMarkdownReply(requestID, content string) ReplyMessage {
	return NewStreamReply(requestID, NewReqID("stream"), content, true)
}

// NewWelcomeTextReply replies to an enter-chat/welcome event with text.
func NewWelcomeTextReply(requestID, content string) ReplyMessage {
	return ReplyMessage{
		Cmd:     WsCmdRespondWelcome,
		Headers: WsHeaders{ReqID: requestID},
		Body: ReplyBody{
			MsgType: MessageTypeText,
			Text:    &TextMessageBody{Content: content},
		},
	}
}

// NewTemplateCardReply replies with a template card.
func NewTemplateCardReply(requestID string, card TemplateCard) ReplyMessage {
	return ReplyMessage{
		Cmd:     WsCmdRespondMessage,
		Headers: WsHeaders{ReqID: requestID},
		Body: ReplyBody{
			MsgType:      MessageTypeTemplateCard,
			TemplateCard: card,
		},
	}
}

// NewUpdateTemplateCard updates an existing template card after a card event.
func NewUpdateTemplateCard(requestID string, card TemplateCard, userIDs []string) ReplyMessage {
	return ReplyMessage{
		Cmd:     WsCmdRespondUpdate,
		Headers: WsHeaders{ReqID: requestID},
		Body: ReplyBody{
			ResponseType: "update_template_card",
			TemplateCard: card,
			UserIDs:      userIDs,
		},
	}
}

// MediaMessageBody references a temporary media_id returned by upload APIs.
type MediaMessageBody struct {
	MediaID string `json:"media_id"`
}

// VideoMessageBody references a temporary video media_id with optional metadata.
type VideoMessageBody struct {
	MediaID     string `json:"media_id"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
}

// NewMediaReply replies with uploaded file/image/voice/video media.
func NewMediaReply(requestID string, mediaType MessageType, mediaID string, videoOptions *VideoMessageBody) ReplyMessage {
	body := ReplyBody{MsgType: mediaType}
	switch mediaType {
	case MessageTypeFile:
		body.File = &MediaMessageBody{MediaID: mediaID}
	case MessageTypeImage:
		body.Image = &MediaMessageBody{MediaID: mediaID}
	case MessageTypeVoice:
		body.Voice = &MediaMessageBody{MediaID: mediaID}
	case MessageTypeVideo:
		if videoOptions == nil {
			videoOptions = &VideoMessageBody{}
		}
		videoOptions.MediaID = mediaID
		body.Video = videoOptions
	}
	return ReplyMessage{
		Cmd:     WsCmdRespondMessage,
		Headers: WsHeaders{ReqID: requestID},
		Body:    body,
	}
}

// ChatType tells the server how to resolve a push target's ChatID
// (official aibot_send_msg field). 主动推送必须显式指定，否则单聊寻址会失败：
// 服务端在 chat_type 缺省时会优先按群聊解析。
type ChatType int

const (
	// ChatTypeSingle 单聊：ChatID 必须传用户的 userid。
	ChatTypeSingle ChatType = 1
	// ChatTypeGroup 群聊：ChatID 传群聊回调里的 chatid。
	ChatTypeGroup ChatType = 2
)

// PushBody proactively sends a message to a chat.
type PushBody struct {
	ChatID string `json:"chatid"`
	// ChatType 指明 ChatID 的会话类型，见 ChatType 常量。由构造函数强制传入。
	ChatType     ChatType             `json:"chat_type,omitempty"`
	MsgType      MessageType          `json:"msgtype"`
	Text         *TextMessageBody     `json:"text,omitempty"`
	Markdown     *MarkdownMessageBody `json:"markdown,omitempty"`
	TemplateCard TemplateCard         `json:"template_card,omitempty"`
	File         *MediaMessageBody    `json:"file,omitempty"`
	Image        *MediaMessageBody    `json:"image,omitempty"`
	Voice        *MediaMessageBody    `json:"voice,omitempty"`
	Video        *VideoMessageBody    `json:"video,omitempty"`
}

// PushMessage is the aibot_send_msg frame.
type PushMessage = WsFrame[PushBody]

// NewTextPush creates a proactive text push payload.
//
// Deprecated: Node SDK and official long-connection docs describe proactive
// send as markdown/template_card/media. Prefer NewMarkdownPush or media push
// helpers when they are available.
func NewTextPush(chatID string, chatType ChatType, content string) PushMessage {
	return PushMessage{
		Cmd:     WsCmdSendMessage,
		Headers: WsHeaders{ReqID: NewReqID(WsCmdSendMessage)},
		Body: PushBody{
			ChatID:   chatID,
			ChatType: chatType,
			MsgType:  MessageTypeText,
			Text:     &TextMessageBody{Content: content},
		},
	}
}

// NewMarkdownPush creates a proactive markdown push payload.
func NewMarkdownPush(chatID string, chatType ChatType, content string) PushMessage {
	return PushMessage{
		Cmd:     WsCmdSendMessage,
		Headers: WsHeaders{ReqID: NewReqID(WsCmdSendMessage)},
		Body: PushBody{
			ChatID:   chatID,
			ChatType: chatType,
			MsgType:  MessageTypeMarkdown,
			Markdown: &MarkdownMessageBody{Content: content},
		},
	}
}

// NewTemplateCardPush proactively sends a template card.
func NewTemplateCardPush(chatID string, chatType ChatType, card TemplateCard) PushMessage {
	return PushMessage{
		Cmd:     WsCmdSendMessage,
		Headers: WsHeaders{ReqID: NewReqID(WsCmdSendMessage)},
		Body: PushBody{
			ChatID:       chatID,
			ChatType:     chatType,
			MsgType:      MessageTypeTemplateCard,
			TemplateCard: card,
		},
	}
}

// NewMediaPush proactively sends uploaded file/image/voice/video media.
func NewMediaPush(chatID string, chatType ChatType, mediaType MessageType, mediaID string, videoOptions *VideoMessageBody) PushMessage {
	body := PushBody{ChatID: chatID, ChatType: chatType, MsgType: mediaType}
	switch mediaType {
	case MessageTypeFile:
		body.File = &MediaMessageBody{MediaID: mediaID}
	case MessageTypeImage:
		body.Image = &MediaMessageBody{MediaID: mediaID}
	case MessageTypeVoice:
		body.Voice = &MediaMessageBody{MediaID: mediaID}
	case MessageTypeVideo:
		if videoOptions == nil {
			videoOptions = &VideoMessageBody{}
		}
		videoOptions.MediaID = mediaID
		body.Video = videoOptions
	}
	return PushMessage{
		Cmd:     WsCmdSendMessage,
		Headers: WsHeaders{ReqID: NewReqID(WsCmdSendMessage)},
		Body:    body,
	}
}

// HeartbeatRequest keeps the long connection alive.
type HeartbeatRequest = WsFrame[struct{}]

// NewHeartbeatRequest builds the heartbeat payload.
func NewHeartbeatRequest() HeartbeatRequest {
	return HeartbeatRequest{
		Cmd:     WsCmdHeartbeat,
		Headers: WsHeaders{ReqID: NewReqID(WsCmdHeartbeat)},
	}
}
