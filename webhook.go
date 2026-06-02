package aibot

import (
	"context"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// WebhookSignature contains the fields used by short-connection callbacks.
type WebhookSignature struct {
	Token     string
	Timestamp string
	Nonce     string
	Payload   string
	Signature string
}

// VerifyWebhookSignature validates the WeCom SHA-1 callback signature.
func VerifyWebhookSignature(in WebhookSignature) bool {
	parts := []string{in.Token, in.Timestamp, in.Nonce, in.Payload}
	sort.Strings(parts)

	h := sha1.New()
	h.Write([]byte(strings.Join(parts, "")))
	expected := hex.EncodeToString(h.Sum(nil))

	// Constant-time comparison avoids leaking how much of the signature matched.
	return subtle.ConstantTimeCompare([]byte(expected), []byte(in.Signature)) == 1
}

// WebhookConfig configures the short-connection callback handler.
type WebhookConfig struct {
	// Token is the callback token configured in WeCom admin console.
	Token string
	// EncodingAESKey enables encrypted callback mode from the WeCom admin console.
	EncodingAESKey string
	// ReceiveID is appended to encrypted payloads. For internal intelligent robots
	// the official callback document says this value is an empty string.
	ReceiveID string
}

// WebhookResponse is the JSON response returned to WeCom in short mode.
type WebhookResponse struct {
	MsgType  MessageType          `json:"msgtype"`
	Text     *TextMessageBody     `json:"text,omitempty"`
	Markdown *MarkdownMessageBody `json:"markdown,omitempty"`
}

// WebhookMessageHandler handles a verified short-connection callback.
type WebhookMessageHandler func(ctx context.Context, msg *Message) (*WebhookResponse, error)

// encryptedWebhookRequest matches WeCom intelligent-robot encrypted callbacks.
// The encrypted string, not the whole JSON body, is the payload used for SHA-1
// signature verification.
type encryptedWebhookRequest struct {
	Encrypt string `json:"encrypt"`
}

// encryptedWebhookResponse matches the encrypted passive-reply envelope in the
// official callback encryption document.
type encryptedWebhookResponse struct {
	Encrypt      string `json:"encrypt"`
	MsgSignature string `json:"msgsignature"`
	Timestamp    int64  `json:"timestamp"`
	Nonce        string `json:"nonce"`
}

// WebhookHandler is an http.Handler for WeCom short-connection callbacks.
type WebhookHandler struct {
	cfg     WebhookConfig
	handler WebhookMessageHandler
}

// NewWebhookHandler creates a callback handler for plaintext or encrypted JSON callbacks.
func NewWebhookHandler(cfg WebhookConfig, handler WebhookMessageHandler) *WebhookHandler {
	return &WebhookHandler{cfg: cfg, handler: handler}
}

// NewWebhookTextResponse creates a plaintext text response for short callbacks.
func NewWebhookTextResponse(content string) *WebhookResponse {
	return &WebhookResponse{
		MsgType: MessageTypeText,
		Text:    &TextMessageBody{Content: content},
	}
}

// NewWebhookMarkdownResponse creates a markdown response for short callbacks.
func NewWebhookMarkdownResponse(content string) *WebhookResponse {
	return &WebhookResponse{
		MsgType:  MessageTypeMarkdown,
		Markdown: &MarkdownMessageBody{Content: content},
	}
}

// ServeHTTP verifies the callback signature before dispatching the message.
func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.cfg.EncodingAESKey != "" {
		h.serveEncrypted(w, r)
		return
	}
	h.servePlaintext(w, r)
}

func (h *WebhookHandler) servePlaintext(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.cfg.Token == "" {
		http.Error(w, "webhook token is required", http.StatusInternalServerError)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}

	query := r.URL.Query()
	signature := WebhookSignature{
		Token:     h.cfg.Token,
		Timestamp: query.Get("timestamp"),
		Nonce:     query.Get("nonce"),
		Payload:   string(body),
		Signature: firstNonEmpty(query.Get("msg_signature"), query.Get("signature")),
	}
	if !VerifyWebhookSignature(signature) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	// 先验签再解析消息，避免非法请求触发业务 JSON 解析和后续处理。
	msg, err := ParseMessage(body)
	if err != nil {
		http.Error(w, "invalid message payload", http.StatusBadRequest)
		return
	}

	var resp *WebhookResponse
	if h.handler != nil {
		resp, err = h.handler(r.Context(), msg)
		if err != nil {
			http.Error(w, "message handler failed", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if resp == nil {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
		return
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "encode response failed", http.StatusInternalServerError)
	}
}

func (h *WebhookHandler) serveEncrypted(w http.ResponseWriter, r *http.Request) {
	crypto, err := NewWecomCrypto(h.cfg.Token, h.cfg.EncodingAESKey, h.cfg.ReceiveID)
	if err != nil {
		http.Error(w, "webhook crypto config is invalid", http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.verifyEncryptedURL(w, r, crypto)
	case http.MethodPost:
		h.handleEncryptedMessage(w, r, crypto)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *WebhookHandler) verifyEncryptedURL(w http.ResponseWriter, r *http.Request, crypto *WecomCrypto) {
	query := r.URL.Query()
	encryptText := query.Get("echostr")
	signature := firstNonEmpty(query.Get("msg_signature"), query.Get("signature"))

	// 101033 的 URL 验证是对 echostr 的密文做签名校验；校验通过后才允许解密，
	// 并且响应体必须是解密后的明文字符串，不能再包 JSON。
	if !crypto.VerifySignature(signature, query.Get("timestamp"), query.Get("nonce"), encryptText) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	plainText, err := crypto.Decrypt(encryptText)
	if err != nil {
		http.Error(w, "decrypt echostr failed", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(plainText))
}

func (h *WebhookHandler) handleEncryptedMessage(w http.ResponseWriter, r *http.Request, crypto *WecomCrypto) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}

	var encryptedReq encryptedWebhookRequest
	if err := json.Unmarshal(body, &encryptedReq); err != nil || encryptedReq.Encrypt == "" {
		http.Error(w, "invalid encrypted message payload", http.StatusBadRequest)
		return
	}

	query := r.URL.Query()
	signature := firstNonEmpty(query.Get("msg_signature"), query.Get("signature"))
	// 加密模式验签的 payload 是 encrypt 字段本身。直接对 JSON body 验签会导致
	// 空白、字段顺序等无关变化影响结果，也不符合企业微信的签名定义。
	if !crypto.VerifySignature(signature, query.Get("timestamp"), query.Get("nonce"), encryptedReq.Encrypt) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	plainText, err := crypto.Decrypt(encryptedReq.Encrypt)
	if err != nil {
		http.Error(w, "decrypt message failed", http.StatusBadRequest)
		return
	}

	// 先把密文还原成官方 JSON 消息体，再复用长连接/明文 webhook 的消息解析结构，
	// 保持两种接入模式的业务处理入口一致。
	msg, err := ParseMessage([]byte(plainText))
	if err != nil {
		http.Error(w, "invalid message payload", http.StatusBadRequest)
		return
	}

	var resp *WebhookResponse
	if h.handler != nil {
		resp, err = h.handler(r.Context(), msg)
		if err != nil {
			http.Error(w, "message handler failed", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if resp == nil {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
		return
	}

	plainResp, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "encode response failed", http.StatusInternalServerError)
		return
	}
	timestamp := responseTimestamp(query.Get("timestamp"))
	nonce := firstNonEmpty(query.Get("nonce"), NewReqID("nonce"))
	encryptedResp, err := crypto.Encrypt(string(plainResp), strconv.FormatInt(timestamp, 10), nonce)
	if err != nil {
		http.Error(w, "encrypt response failed", http.StatusInternalServerError)
		return
	}

	out := encryptedWebhookResponse{
		Encrypt:      encryptedResp.Encrypt,
		MsgSignature: encryptedResp.Signature,
		Timestamp:    timestamp,
		Nonce:        nonce,
	}
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, "encode encrypted response failed", http.StatusInternalServerError)
	}
}

func responseTimestamp(value string) int64 {
	if value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsed
		}
	}
	return time.Now().Unix()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
