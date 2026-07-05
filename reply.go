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

// StreamMessageBody is the body of a stream reply.
type StreamMessageBody struct {
	ID      string `json:"id"`
	Finish  bool   `json:"finish,omitempty"`
	Content string `json:"content,omitempty"`
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
