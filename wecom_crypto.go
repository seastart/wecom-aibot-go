package aibot

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const wecomPKCS7BlockSize = 32

// WecomCrypto handles encrypted short-connection callback payloads.
type WecomCrypto struct {
	token     string
	aesKey    []byte
	iv        []byte
	receiveID string
}

// EncryptedMessage is the encrypted text and its signature.
type EncryptedMessage struct {
	Encrypt   string
	Signature string
}

// NewWecomCrypto creates a WeCom callback crypto helper.
func NewWecomCrypto(token, encodingAESKey, receiveID string) (*WecomCrypto, error) {
	if token == "" {
		return nil, errors.New("wecom crypto: token is required")
	}
	key, err := decodeEncodingAESKey(encodingAESKey)
	if err != nil {
		return nil, err
	}
	return &WecomCrypto{
		token:     token,
		aesKey:    key,
		iv:        key[:aes.BlockSize],
		receiveID: receiveID,
	}, nil
}

// ComputeSignature computes the SHA1 signature for encrypted callbacks.
func (c *WecomCrypto) ComputeSignature(timestamp, nonce, encrypt string) string {
	parts := []string{c.token, timestamp, nonce, encrypt}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, "")))
	return hex.EncodeToString(sum[:])
}

// VerifySignature verifies the SHA1 signature in constant time.
func (c *WecomCrypto) VerifySignature(signature, timestamp, nonce, encrypt string) bool {
	expected := c.ComputeSignature(timestamp, nonce, encrypt)
	return subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) == 1
}

// Decrypt decrypts a base64 encrypted callback payload.
func (c *WecomCrypto) Decrypt(encryptText string) (string, error) {
	encrypted, err := base64.StdEncoding.DecodeString(encryptText)
	if err != nil {
		return "", fmt.Errorf("wecom crypto: decode encrypt text: %w", err)
	}
	if len(encrypted)%aes.BlockSize != 0 {
		return "", fmt.Errorf("wecom crypto: encrypted length %d is not AES block aligned", len(encrypted))
	}

	block, err := aes.NewCipher(c.aesKey)
	if err != nil {
		return "", err
	}
	decrypted := make([]byte, len(encrypted))
	cipher.NewCBCDecrypter(block, c.iv).CryptBlocks(decrypted, encrypted)

	raw, err := pkcs7Unpad(decrypted, wecomPKCS7BlockSize)
	if err != nil {
		return "", err
	}
	if len(raw) < 20 {
		return "", fmt.Errorf("wecom crypto: invalid payload length %d", len(raw))
	}

	msgLen := int(binary.BigEndian.Uint32(raw[16:20]))
	msgStart := 20
	msgEnd := msgStart + msgLen
	if msgEnd > len(raw) {
		return "", fmt.Errorf("wecom crypto: invalid message length %d", msgLen)
	}

	message := string(raw[msgStart:msgEnd])
	if c.receiveID != "" {
		got := string(raw[msgEnd:])
		if got != c.receiveID {
			return "", fmt.Errorf("wecom crypto: receive id mismatch: expected %q got %q", c.receiveID, got)
		}
	}
	return message, nil
}

// Encrypt encrypts plaintext and computes its callback signature.
func (c *WecomCrypto) Encrypt(plainText, timestamp, nonce string) (*EncryptedMessage, error) {
	random16 := make([]byte, 16)
	if _, err := rand.Read(random16); err != nil {
		return nil, err
	}

	msg := []byte(plainText)
	msgLen := make([]byte, 4)
	binary.BigEndian.PutUint32(msgLen, uint32(len(msg)))
	raw := bytes.Join([][]byte{random16, msgLen, msg, []byte(c.receiveID)}, nil)
	padded := pkcs7Pad(raw, wecomPKCS7BlockSize)

	block, err := aes.NewCipher(c.aesKey)
	if err != nil {
		return nil, err
	}
	encrypted := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, c.iv).CryptBlocks(encrypted, padded)

	encryptText := base64.StdEncoding.EncodeToString(encrypted)
	return &EncryptedMessage{
		Encrypt:   encryptText,
		Signature: c.ComputeSignature(timestamp, nonce, encryptText),
	}, nil
}

func decodeEncodingAESKey(encodingAESKey string) ([]byte, error) {
	trimmed := strings.TrimSpace(encodingAESKey)
	if trimmed == "" {
		return nil, errors.New("wecom crypto: encoding aes key is required")
	}
	if !strings.HasSuffix(trimmed, "=") {
		trimmed += "="
	}
	key, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("wecom crypto: aes key length = %d, want 32", len(key))
	}
	return key, nil
}

func pkcs7Pad(data []byte, blockSize int) []byte {
	padLen := blockSize - len(data)%blockSize
	if padLen == 0 {
		padLen = blockSize
	}
	out := make([]byte, 0, len(data)+padLen)
	out = append(out, data...)
	out = append(out, bytes.Repeat([]byte{byte(padLen)}, padLen)...)
	return out
}

func pkcs7Unpad(data []byte, blockSize int) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("pkcs7 unpad: empty buffer")
	}
	padLen := int(data[len(data)-1])
	if padLen < 1 || padLen > blockSize || padLen > len(data) {
		return nil, fmt.Errorf("pkcs7 unpad: invalid padding length %d", padLen)
	}
	for i := len(data) - padLen; i < len(data); i++ {
		if int(data[i]) != padLen {
			return nil, errors.New("pkcs7 unpad: padding bytes mismatch")
		}
	}
	return data[:len(data)-padLen], nil
}
