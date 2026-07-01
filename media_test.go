package aibot

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDecryptFile(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	plain := []byte("hello encrypted media")
	encrypted := encryptForTest(t, plain, key)

	got, err := DecryptFile(encrypted, base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatalf("DecryptFile returned error: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("decrypted = %q, want %q", got, plain)
	}
}

func TestDecryptFileRejectsBadPadding(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	encrypted := encryptForTest(t, []byte("hello"), key)
	encrypted[len(encrypted)-1] = 99

	_, err := DecryptFile(encrypted, base64.StdEncoding.EncodeToString(key))
	if err == nil {
		t.Fatalf("DecryptFile should reject invalid padding")
	}
}

func TestDownloadFileDecryptsWhenAESKeyProvided(t *testing.T) {
	key := bytes.Repeat([]byte{9}, 32)
	plain := []byte("downloaded media")
	encrypted := encryptForTest(t, plain, key)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Disposition", `attachment; filename="media.bin"`)
		_, _ = w.Write(encrypted)
	}))
	defer server.Close()

	got, err := DownloadFile(context.Background(), server.URL, base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatalf("DownloadFile returned error: %v", err)
	}
	if got.Filename != "media.bin" {
		t.Fatalf("Filename = %q, want media.bin", got.Filename)
	}
	if !bytes.Equal(got.Buffer, plain) {
		t.Fatalf("Buffer = %q, want %q", got.Buffer, plain)
	}
}

func encryptForTest(t *testing.T, plain []byte, key []byte) []byte {
	t.Helper()

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher returned error: %v", err)
	}

	padded := pkcs7PadForTest(plain, 32)
	encrypted := make([]byte, len(padded))
	mode := cipher.NewCBCEncrypter(block, key[:aes.BlockSize])
	mode.CryptBlocks(encrypted, padded)
	return encrypted
}

func pkcs7PadForTest(data []byte, blockSize int) []byte {
	padLen := blockSize - len(data)%blockSize
	out := make([]byte, 0, len(data)+padLen)
	out = append(out, data...)
	out = append(out, bytes.Repeat([]byte{byte(padLen)}, padLen)...)
	return out
}

// TestDecodeBase64Lenient 验证宽容解码：无填充、URL-safe、标准填充三种形态都能还原 32 字节 key。
// 复现真机 bug：企微 aeskey 是 43 字符无填充 base64，严格 StdEncoding 会在 byte 40 处报错。
func TestDecodeBase64Lenient(t *testing.T) {
	var key [32]byte
	for i := range key {
		key[i] = byte(i * 7)
	}
	cases := map[string]string{
		"raw-std（无填充，复现真机 43 字符）": base64.RawStdEncoding.EncodeToString(key[:]),
		"raw-url（URL-safe 无填充）":   base64.RawURLEncoding.EncodeToString(key[:]),
		"std（标准带填充）":              base64.StdEncoding.EncodeToString(key[:]),
	}
	for name, enc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := decodeBase64Lenient(enc)
			if err != nil {
				t.Fatalf("解码 %q 失败: %v", enc, err)
			}
			if !bytes.Equal(got, key[:]) {
				t.Errorf("解码结果与原 key 不一致")
			}
		})
	}
}
