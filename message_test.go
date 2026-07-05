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

// 官方事件字段嵌套在 event.<eventtype> 子对象里（见文档 101027），
// 这里验证 template_card_event / feedback_event 的关键字段能被正确解析。
func TestParseFrameEventNestedFields(t *testing.T) {
	t.Run("template_card_event", func(t *testing.T) {
		raw := `{"cmd":"aibot_event_callback","headers":{"req_id":"r-1"},"body":{"msgtype":"event","event":{"eventtype":"template_card_event","template_card_event":{"card_type":"button_interaction","event_key":"btn_ok","task_id":"T1","selected_items":{"selected_item":[{"question_key":"q1","option_ids":{"option_id":["a","b"]}}]}}}}}`
		frame, err := ParseFrame([]byte(raw))
		if err != nil {
			t.Fatalf("ParseFrame returned error: %v", err)
		}
		card := frame.Event.Event.TemplateCard
		if card == nil {
			t.Fatalf("TemplateCard should be parsed")
		}
		if card.EventKey != "btn_ok" || card.TaskID != "T1" || card.CardType != "button_interaction" {
			t.Fatalf("unexpected card fields: %+v", card)
		}
		if card.SelectedItems == nil || len(card.SelectedItems.SelectedItem) != 1 {
			t.Fatalf("selected_items should be parsed: %+v", card.SelectedItems)
		}
		if got := card.SelectedItems.SelectedItem[0].OptionIDs.OptionID; len(got) != 2 || got[0] != "a" {
			t.Fatalf("unexpected option_ids: %v", got)
		}
	})

	t.Run("feedback_event", func(t *testing.T) {
		raw := `{"cmd":"aibot_event_callback","headers":{"req_id":"r-1"},"body":{"msgtype":"event","event":{"eventtype":"feedback_event","feedback_event":{"id":"FB1","type":2,"content":"再详细些","inaccurate_reason_list":[2,4]}}}}`
		frame, err := ParseFrame([]byte(raw))
		if err != nil {
			t.Fatalf("ParseFrame returned error: %v", err)
		}
		fb := frame.Event.Event.Feedback
		if fb == nil {
			t.Fatalf("Feedback should be parsed")
		}
		if fb.ID != "FB1" || fb.Type != 2 || fb.Content != "再详细些" {
			t.Fatalf("unexpected feedback fields: %+v", fb)
		}
		if len(fb.InaccurateReasonList) != 2 || fb.InaccurateReasonList[0] != 2 {
			t.Fatalf("unexpected inaccurate_reason_list: %v", fb.InaccurateReasonList)
		}
	})
}
