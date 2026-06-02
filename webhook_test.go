package aibot

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

func TestVerifyWebhookSignature(t *testing.T) {
	ok := VerifyWebhookSignature(WebhookSignature{
		Token:     "token",
		Timestamp: "1710000000",
		Nonce:     "nonce",
		Payload:   "payload",
		Signature: "8c5365de55212e2e64c8d267ffbddc41f2d646d7",
	})
	if !ok {
		t.Fatalf("signature should be valid")
	}
}

func TestVerifyWebhookSignatureRejectsMismatch(t *testing.T) {
	ok := VerifyWebhookSignature(WebhookSignature{
		Token:     "token",
		Timestamp: "1710000000",
		Nonce:     "nonce",
		Payload:   "payload",
		Signature: "bad",
	})
	if ok {
		t.Fatalf("signature should be rejected")
	}
}

func TestWebhookHandlerAcceptsSignedMessage(t *testing.T) {
	body := []byte(`{"msgtype":"text","msgid":"msg-1","text":{"content":"hello"}}`)
	called := make(chan *Message, 1)
	handler := NewWebhookHandler(WebhookConfig{Token: "token"}, func(ctx context.Context, msg *Message) (*WebhookResponse, error) {
		called <- msg
		return NewWebhookTextResponse("ok"), nil
	})

	req := httptest.NewRequest(http.MethodPost, "/callback?timestamp=1710000000&nonce=nonce&msg_signature=e9b89355c4fe1b55d0616c7906986dae2d512d8d", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	select {
	case msg := <-called:
		if msg.Text == nil || msg.Text.Content != "hello" {
			t.Fatalf("message = %#v, want text hello", msg)
		}
	default:
		t.Fatalf("message handler was not called")
	}
}

func TestWebhookHandlerRejectsBadSignature(t *testing.T) {
	body := []byte(`{"msgtype":"text","text":{"content":"hello"}}`)
	handler := NewWebhookHandler(WebhookConfig{Token: "token"}, func(ctx context.Context, msg *Message) (*WebhookResponse, error) {
		t.Fatalf("handler should not be called")
		return nil, nil
	})

	req := httptest.NewRequest(http.MethodPost, "/callback?timestamp=1710000000&nonce=nonce&msg_signature=bad", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestWebhookHandlerValidatesEncryptedURL(t *testing.T) {
	const (
		token = "token"
		key   = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG"
		ts    = "1710000000"
		nonce = "nonce"
	)
	crypto, err := NewWecomCrypto(token, key, "")
	if err != nil {
		t.Fatalf("NewWecomCrypto() error = %v", err)
	}
	encrypted, err := crypto.Encrypt("pong", ts, nonce)
	if err != nil {
		t.Fatalf("Encrypt() error = %v", err)
	}

	handler := NewWebhookHandler(WebhookConfig{Token: token, EncodingAESKey: key}, nil)
	reqURL := "/callback?timestamp=" + ts + "&nonce=" + nonce + "&msg_signature=" + encrypted.Signature + "&echostr=" + url.QueryEscape(encrypted.Encrypt)
	req := httptest.NewRequest(http.MethodGet, reqURL, nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Body.String() != "pong" {
		t.Fatalf("body = %q, want %q", rec.Body.String(), "pong")
	}
}

func TestWebhookHandlerAcceptsEncryptedJSONMessageAndEncryptsResponse(t *testing.T) {
	const (
		token = "token"
		key   = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG"
		ts    = "1710000000"
		nonce = "nonce"
	)
	crypto, err := NewWecomCrypto(token, key, "")
	if err != nil {
		t.Fatalf("NewWecomCrypto() error = %v", err)
	}
	plainBody := `{"msgtype":"text","msgid":"msg-1","text":{"content":"hello"}}`
	encryptedReq, err := crypto.Encrypt(plainBody, ts, nonce)
	if err != nil {
		t.Fatalf("Encrypt(request) error = %v", err)
	}
	wireBody, err := json.Marshal(map[string]string{"encrypt": encryptedReq.Encrypt})
	if err != nil {
		t.Fatalf("Marshal(request) error = %v", err)
	}

	called := make(chan *Message, 1)
	handler := NewWebhookHandler(WebhookConfig{Token: token, EncodingAESKey: key}, func(ctx context.Context, msg *Message) (*WebhookResponse, error) {
		called <- msg
		return NewWebhookTextResponse("ok"), nil
	})
	req := httptest.NewRequest(http.MethodPost, "/callback?timestamp="+ts+"&nonce="+nonce+"&msg_signature="+encryptedReq.Signature, bytes.NewReader(wireBody))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	select {
	case msg := <-called:
		if msg.Text == nil || msg.Text.Content != "hello" {
			t.Fatalf("message = %#v, want text hello", msg)
		}
	default:
		t.Fatalf("message handler was not called")
	}

	var encryptedResp encryptedWebhookResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &encryptedResp); err != nil {
		t.Fatalf("Unmarshal(response) error = %v, body=%s", err, rec.Body.String())
	}
	if encryptedResp.Encrypt == "" || encryptedResp.MsgSignature == "" {
		t.Fatalf("encrypted response missing fields: %#v", encryptedResp)
	}
	if !crypto.VerifySignature(encryptedResp.MsgSignature, strconv.FormatInt(encryptedResp.Timestamp, 10), encryptedResp.Nonce, encryptedResp.Encrypt) {
		t.Fatalf("encrypted response signature is invalid")
	}
	decryptedResp, err := crypto.Decrypt(encryptedResp.Encrypt)
	if err != nil {
		t.Fatalf("Decrypt(response) error = %v", err)
	}
	var resp WebhookResponse
	if err := json.Unmarshal([]byte(decryptedResp), &resp); err != nil {
		t.Fatalf("Unmarshal(decrypted response) error = %v, body=%s", err, decryptedResp)
	}
	if resp.Text == nil || resp.Text.Content != "ok" {
		t.Fatalf("response = %#v, want text ok", resp)
	}
}
