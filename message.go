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
type EventContent struct {
	EventType string          `json:"eventtype,omitempty"`
	TaskID    string          `json:"task_id,omitempty"`
	Raw       json.RawMessage `json:"-"`
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
