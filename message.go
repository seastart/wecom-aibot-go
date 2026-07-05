package aibot

import (
	"encoding/json"
	"errors"
)

// MessageType is the enterprise WeCom robot message type.
// Unknown values are preserved so callers can add their own handling before
// this library learns about newly released message formats.
type MessageType string

const (
	MessageTypeText         MessageType = "text"
	MessageTypeMarkdown     MessageType = "markdown"
	MessageTypeImage        MessageType = "image"
	MessageTypeMixed        MessageType = "mixed"
	MessageTypeFile         MessageType = "file"
	MessageTypeVoice        MessageType = "voice"
	MessageTypeVideo        MessageType = "video"
	MessageTypeEvent        MessageType = "event"
	MessageTypeStream       MessageType = "stream"
	MessageTypeTemplateCard MessageType = "template_card"
)

// 事件类型（eventtype），统一通过 aibot_event_callback 下发。
// 未知取值会原样保留在 EventContent.EventType 里，方便调用方在库支持新事件前
// 自行处理。参考官方文档：事件回调 https://developer.work.weixin.qq.com/document/path/101027
const (
	// EventTypeEnterChat 用户当天首次进入机器人单聊会话，可回复欢迎语。
	EventTypeEnterChat = "enter_chat"
	// EventTypeTemplateCard 用户点击模板卡片的按钮/选项，需在 5 秒内响应，否则连接断开。
	EventTypeTemplateCard = "template_card_event"
	// EventTypeFeedback 用户对机器人回复做出点赞/点踩反馈，仅支持回复空包。
	EventTypeFeedback = "feedback_event"
	// EventTypeDisconnected 有新连接建立时，服务端向旧连接推送此事件并主动断开。
	// 属于长连接控制事件（见长连接文档 101463），收到后不应继续重连。
	EventTypeDisconnected = "disconnected_event"
)

// FrameKind describes the high-level payload pushed by the long connection.
type FrameKind string

const (
	FrameKindMessage FrameKind = "message"
	FrameKindEvent   FrameKind = "event"
	FrameKindAck     FrameKind = "ack"
	FrameKindUnknown FrameKind = "unknown"
)

// User identifies the sender of a message.
type User struct {
	UserID string `json:"userid,omitempty"`
	Name   string `json:"name,omitempty"`
	Alias  string `json:"alias,omitempty"`
}

// TextContent is the body of a text message.
type TextContent struct {
	Content string `json:"content"`
}

// MarkdownContent is the body of a markdown message.
type MarkdownContent struct {
	Content string `json:"content"`
}

// MediaContent carries media identifiers from image/file/voice/video messages.
type MediaContent struct {
	// URL is present in received media messages. It is usually short-lived.
	URL string `json:"url,omitempty"`
	// AESKey is returned by long-connection media messages for decrypting URL content.
	AESKey string `json:"aeskey,omitempty"`
	// MediaID is used when replying or proactively sending uploaded media.
	MediaID     string `json:"media_id,omitempty"`
	FileName    string `json:"filename,omitempty"`
	MimeType    string `json:"mime_type,omitempty"`
	Size        int64  `json:"size,omitempty"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
}

// VoiceContent is special because received voice messages include recognized text.
type VoiceContent struct {
	Content string `json:"content,omitempty"`
	URL     string `json:"url,omitempty"`
	AESKey  string `json:"aeskey,omitempty"`
}

// MixedMsgItem is one item of a mixed text/image message.
type MixedMsgItem struct {
	MsgType MessageType     `json:"msgtype"`
	Text    *TextContent    `json:"text,omitempty"`
	Image   *MediaContent   `json:"image,omitempty"`
	Raw     json.RawMessage `json:"-"`
}

// MixedContent is a mixed text/image message body.
type MixedContent struct {
	MsgItem []MixedMsgItem `json:"msg_item"`
}

// QuoteContent is present when the user quotes another message.
type QuoteContent struct {
	MsgType MessageType     `json:"msgtype"`
	Text    *TextContent    `json:"text,omitempty"`
	Image   *MediaContent   `json:"image,omitempty"`
	Mixed   *MixedContent   `json:"mixed,omitempty"`
	Voice   *VoiceContent   `json:"voice,omitempty"`
	File    *MediaContent   `json:"file,omitempty"`
	Raw     json.RawMessage `json:"-"`
}

// Message is the normalized message payload delivered by WeCom.
// Raw intentionally keeps the exact JSON object for fields not modeled here.
type Message struct {
	ReqID       string           `json:"-"`
	Type        MessageType      `json:"msgtype"`
	ID          string           `json:"msgid,omitempty"`
	AIBotID     string           `json:"aibotid,omitempty"`
	ChatID      string           `json:"chatid,omitempty"`
	ChatType    string           `json:"chattype,omitempty"`
	From        *User            `json:"from,omitempty"`
	CreateTime  int64            `json:"create_time,omitempty"`
	ResponseURL string           `json:"response_url,omitempty"`
	Text        *TextContent     `json:"text,omitempty"`
	Markdown    *MarkdownContent `json:"markdown,omitempty"`
	Image       *MediaContent    `json:"image,omitempty"`
	Mixed       *MixedContent    `json:"mixed,omitempty"`
	File        *MediaContent    `json:"file,omitempty"`
	Voice       *VoiceContent    `json:"voice,omitempty"`
	Video       *MediaContent    `json:"video,omitempty"`
	Quote       *QuoteContent    `json:"quote,omitempty"`
	Raw         json.RawMessage  `json:"-"`
}

// Event is a normalized event payload delivered by WeCom.
type Event struct {
	ReqID      string          `json:"-"`
	ID         string          `json:"msgid,omitempty"`
	Type       MessageType     `json:"msgtype,omitempty"`
	AIBotID    string          `json:"aibotid,omitempty"`
	ChatID     string          `json:"chatid,omitempty"`
	ChatType   string          `json:"chattype,omitempty"`
	From       *User           `json:"from,omitempty"`
	CreateTime int64           `json:"create_time,omitempty"`
	Event      *EventContent   `json:"event,omitempty"`
	Raw        json.RawMessage `json:"-"`
}

// EventContent carries the concrete event name and protocol-specific fields.
// 不同事件的业务字段分别嵌套在与 eventtype 同名的子对象里（官方文档 101027），
// 例如 template_card_event 的内容在 event.template_card_event 下，而不是与
// eventtype 平级，所以这里用独立的子结构体建模，不能把字段直接挂在本结构上。
type EventContent struct {
	// EventType 事件类型，取值见 EventType* 常量；未知取值原样保留。
	EventType string `json:"eventtype,omitempty"`
	// TemplateCard 模板卡片交互详情，仅 eventtype=template_card_event 时存在。
	TemplateCard *TemplateCardEvent `json:"template_card_event,omitempty"`
	// Feedback 用户反馈详情，仅 eventtype=feedback_event 时存在。
	Feedback *FeedbackEvent  `json:"feedback_event,omitempty"`
	Raw      json.RawMessage `json:"-"`
}

// TemplateCardEvent 是用户点击模板卡片按钮/选项后回调的事件详情。
type TemplateCardEvent struct {
	// CardType 卡片类型：button_interaction / vote_interaction /
	// multiple_interaction / text_notice / news_notice。
	CardType string `json:"card_type,omitempty"`
	// EventKey 用户点击的按钮 key，对应下发卡片时设置的 key。
	EventKey string `json:"event_key,omitempty"`
	// TaskID 交互卡片的 task_id，更新卡片时需回传相同值。
	TaskID string `json:"task_id,omitempty"`
	// SelectedItems 投票/多选类卡片的用户选择结果，普通按钮卡片不返回。
	SelectedItems *SelectedItems `json:"selected_items,omitempty"`
}

// SelectedItems 保留企业微信 XML 转 JSON 后的双层嵌套：selected_items.selected_item[]。
type SelectedItems struct {
	SelectedItem []SelectedItem `json:"selected_item,omitempty"`
}

// SelectedItem 是一道题的选择结果。
type SelectedItem struct {
	// QuestionKey 题目 key。
	QuestionKey string `json:"question_key,omitempty"`
	// OptionIDs 用户选中的选项 id，同样是双层嵌套：option_ids.option_id[]。
	OptionIDs *OptionIDs `json:"option_ids,omitempty"`
}

// OptionIDs 保留 option_id 数组外层的包裹结构。
type OptionIDs struct {
	OptionID []string `json:"option_id,omitempty"`
}

// FeedbackEvent 是用户对机器人回复做出反馈时的事件详情。
type FeedbackEvent struct {
	// ID 反馈 id，对应回复消息时设置的 stream.feedback.id。
	ID string `json:"id,omitempty"`
	// Type 反馈类型：1=准确（点赞），2=不准确（点踩），3=取消反馈。
	Type int `json:"type,omitempty"`
	// Content 用户填写的反馈内容，仅 type=2（不准确）时返回。
	Content string `json:"content,omitempty"`
	// InaccurateReasonList 负反馈原因编号列表，仅 type=2 时返回。
	InaccurateReasonList []int `json:"inaccurate_reason_list,omitempty"`
}

// Ack is returned by WeCom for client-side requests such as subscribe/reply.
type Ack struct {
	ErrCode   int             `json:"errcode"`
	ErrMsg    string          `json:"errmsg,omitempty"`
	RequestID string          `json:"request_id,omitempty"`
	ReqID     string          `json:"-"`
	Body      json.RawMessage `json:"body,omitempty"`
}

// Frame is the top-level result of parsing one WebSocket frame.
type Frame struct {
	Kind    FrameKind
	Message *Message
	Event   *Event
	Ack     *Ack
	Raw     json.RawMessage
}

// ParseMessage parses one message payload and validates the routing field.
func ParseMessage(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	if msg.Type == "" {
		return nil, errors.New("wecom aibot: missing msgtype")
	}
	msg.Raw = append(json.RawMessage(nil), data...)
	if msg.Quote != nil {
		msg.Quote.Raw = append(json.RawMessage(nil), msg.Raw...)
	}
	return &msg, nil
}

// ParseFrame detects whether a WebSocket frame is a message, event, or ack.
func ParseFrame(data []byte) (*Frame, error) {
	var ws struct {
		Cmd     string          `json:"cmd"`
		Headers WsHeaders       `json:"headers"`
		Body    json.RawMessage `json:"body"`
		ErrCode *int            `json:"errcode"`
		ErrMsg  string          `json:"errmsg"`
	}
	if err := json.Unmarshal(data, &ws); err != nil {
		return nil, err
	}

	frame := &Frame{Kind: FrameKindUnknown, Raw: append(json.RawMessage(nil), data...)}

	// 长连接协议外层是 {cmd, headers, body}。必须先按 cmd 分流，
	// 再解析 body，否则会把回执帧和业务消息混在一起。
	switch ws.Cmd {
	case WsCmdMessageCallback:
		msg, err := ParseMessage(ws.Body)
		if err != nil {
			return nil, err
		}
		frame.Kind = FrameKindMessage
		frame.Message = msg
		frame.Message.ReqID = ws.Headers.ReqID
	case WsCmdEventCallback:
		var event Event
		if err := json.Unmarshal(ws.Body, &event); err != nil {
			return nil, err
		}
		event.Raw = append(json.RawMessage(nil), ws.Body...)
		frame.Kind = FrameKindEvent
		frame.Event = &event
		frame.Event.ReqID = ws.Headers.ReqID
	case "":
		if ws.ErrCode == nil {
			return frame, nil
		}
		errCode := *ws.ErrCode
		ack := Ack{ErrCode: errCode, ErrMsg: ws.ErrMsg, ReqID: ws.Headers.ReqID, RequestID: ws.Headers.ReqID, Body: ws.Body}
		frame.Kind = FrameKindAck
		frame.Ack = &ack
	}

	return frame, nil
}
