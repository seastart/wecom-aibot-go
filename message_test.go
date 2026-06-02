package aibot

import "testing"

func TestParseMessageKeepsKnownAndRawFields(t *testing.T) {
	payload := []byte(`{
		"msgtype":"text",
		"msgid":"msg-1",
		"chatid":"chat-1",
		"from":{"userid":"u-1"},
		"text":{"content":"hello"},
		"quote":{"msgtype":"text","text":{"content":"old"}},
		"unknown":{"nested":true}
	}`)

	msg, err := ParseMessage(payload)
	if err != nil {
		t.Fatalf("ParseMessage returned error: %v", err)
	}

	if msg.Type != MessageTypeText {
		t.Fatalf("Type = %q, want %q", msg.Type, MessageTypeText)
	}
	if msg.Text == nil || msg.Text.Content != "hello" {
		t.Fatalf("Text content = %#v, want hello", msg.Text)
	}
	if msg.Quote == nil || msg.Quote.Text == nil || msg.Quote.Text.Content != "old" {
		t.Fatalf("Quote = %#v, want quoted text old", msg.Quote)
	}
	if len(msg.Raw) == 0 {
		t.Fatalf("Raw should keep original payload for forward compatibility")
	}
}

func TestParseMessageRejectsMissingType(t *testing.T) {
	_, err := ParseMessage([]byte(`{"msgid":"msg-1"}`))
	if err == nil {
		t.Fatalf("ParseMessage should reject payload without msgtype")
	}
}

func TestParseFrameDetectsMessageEventAndAck(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		kind FrameKind
	}{
		{name: "message", raw: `{"cmd":"aibot_msg_callback","headers":{"req_id":"r-1"},"body":{"msgtype":"text","text":{"content":"hello"}}}`, kind: FrameKindMessage},
		{name: "event", raw: `{"cmd":"aibot_event_callback","headers":{"req_id":"r-1"},"body":{"msgtype":"event","event":{"eventtype":"disconnected_event"}}}`, kind: FrameKindEvent},
		{name: "ack", raw: `{"headers":{"req_id":"r-1"},"errcode":0,"errmsg":"ok"}`, kind: FrameKindAck},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := ParseFrame([]byte(tc.raw))
			if err != nil {
				t.Fatalf("ParseFrame returned error: %v", err)
			}
			if frame.Kind != tc.kind {
				t.Fatalf("Kind = %q, want %q", frame.Kind, tc.kind)
			}
			if frame.Message != nil && frame.Message.ReqID == "" {
				t.Fatalf("Message.ReqID should be copied from frame headers")
			}
		})
	}
}
