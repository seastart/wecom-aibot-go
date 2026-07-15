package aibot

import (
	"encoding/json"
	"testing"
)

func TestSubscribeRequestUsesBotCredentials(t *testing.T) {
	req := NewSubscribeRequest("bot-1", "secret-1")

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	if got["cmd"] != "aibot_subscribe" {
		t.Fatalf("cmd = %v, want aibot_subscribe", got["cmd"])
	}
	body := got["body"].(map[string]any)
	if body["bot_id"] != "bot-1" {
		t.Fatalf("body.bot_id = %v, want bot-1", body["bot_id"])
	}
	if body["secret"] != "secret-1" {
		t.Fatalf("body.secret = %v, want secret-1", body["secret"])
	}
}

func TestTextReplyBuildsStreamReplyPayload(t *testing.T) {
	reply := NewTextReply("msg-1", "received")

	data, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	if got["cmd"] != "aibot_respond_msg" {
		t.Fatalf("cmd = %v, want aibot_respond_msg", got["cmd"])
	}
	headers := got["headers"].(map[string]any)
	if headers["req_id"] != "msg-1" {
		t.Fatalf("headers.req_id = %v, want msg-1", headers["req_id"])
	}
	body := got["body"].(map[string]any)
	if body["msgtype"] != "stream" {
		t.Fatalf("body.msgtype = %v, want stream", body["msgtype"])
	}
	stream := body["stream"].(map[string]any)
	if stream["content"] != "received" {
		t.Fatalf("stream.content = %v, want received", stream["content"])
	}
	if stream["finish"] != true {
		t.Fatalf("stream.finish = %v, want true", stream["finish"])
	}
}

func TestStreamReplyBuildsIncrementalPayload(t *testing.T) {
	reply := NewStreamReply("req-1", "stream-1", "thinking", false)

	data, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	body := got["body"].(map[string]any)
	stream := body["stream"].(map[string]any)
	if stream["id"] != "stream-1" {
		t.Fatalf("stream.id = %v, want stream-1", stream["id"])
	}
	if stream["content"] != "thinking" {
		t.Fatalf("stream.content = %v, want thinking", stream["content"])
	}
	if _, ok := stream["finish"]; ok {
		t.Fatalf("stream.finish should be omitted when false")
	}
}

func TestStreamReplyWithFeedbackAttachesFeedbackID(t *testing.T) {
	reply := NewStreamReplyWithFeedback("req-1", "stream-1", "vinezing...", "fb-42", false)

	data, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	stream := got["body"].(map[string]any)["stream"].(map[string]any)
	feedback, ok := stream["feedback"].(map[string]any)
	if !ok {
		t.Fatalf("stream.feedback missing, got %v", stream)
	}
	if feedback["id"] != "fb-42" {
		t.Fatalf("stream.feedback.id = %v, want fb-42", feedback["id"])
	}
}

func TestStreamReplyWithEmptyFeedbackOmitsFeedback(t *testing.T) {
	reply := NewStreamReplyWithFeedback("req-1", "stream-1", "hi", "", true)

	data, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	stream := got["body"].(map[string]any)["stream"].(map[string]any)
	if _, ok := stream["feedback"]; ok {
		t.Fatalf("stream.feedback should be omitted when feedbackID empty")
	}
}

func TestPushMarkdownBuildsPushPayload(t *testing.T) {
	push := NewMarkdownPush("chat-1", ChatTypeGroup, "**done**")

	data, err := json.Marshal(push)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	if got["cmd"] != "aibot_send_msg" {
		t.Fatalf("cmd = %v, want aibot_send_msg", got["cmd"])
	}
	body := got["body"].(map[string]any)
	if body["chatid"] != "chat-1" {
		t.Fatalf("body.chatid = %v, want chat-1", body["chatid"])
	}
	// chat_type 必须随包发出，否则单聊寻址会被服务端按群聊解析。
	if body["chat_type"] != float64(ChatTypeGroup) {
		t.Fatalf("body.chat_type = %v, want %d", body["chat_type"], ChatTypeGroup)
	}
}

func TestWelcomeTextReplyBuildsWelcomeCommand(t *testing.T) {
	reply := NewWelcomeTextReply("req-1", "hi")

	data, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if got["cmd"] != "aibot_respond_welcome_msg" {
		t.Fatalf("cmd = %v, want aibot_respond_welcome_msg", got["cmd"])
	}
}

func TestReplyMediaBuildsMediaPayload(t *testing.T) {
	reply := NewMediaReply("req-1", MessageTypeImage, "media-1", nil)

	data, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	body := got["body"].(map[string]any)
	image := body["image"].(map[string]any)
	if image["media_id"] != "media-1" {
		t.Fatalf("image.media_id = %v, want media-1", image["media_id"])
	}
}

func TestUpdateTemplateCardBuildsUpdateCommand(t *testing.T) {
	reply := NewUpdateTemplateCard("req-1", TemplateCard{"card_type": "text_notice"}, []string{"u-1"})

	data, err := json.Marshal(reply)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if got["cmd"] != "aibot_respond_update_msg" {
		t.Fatalf("cmd = %v, want aibot_respond_update_msg", got["cmd"])
	}
	body := got["body"].(map[string]any)
	if body["response_type"] != "update_template_card" {
		t.Fatalf("response_type = %v, want update_template_card", body["response_type"])
	}
}
