package aibot

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"
)

// DownloadedFile is the result of downloading and optionally decrypting media.
type DownloadedFile struct {
	Buffer   []byte
	Filename string
}

// DecryptFile decrypts a WeCom encrypted media file with AES-256-CBC.
func DecryptFile(encrypted []byte, aesKey string) ([]byte, error) {
	if len(encrypted) == 0 {
		return nil, errors.New("decrypt file: encrypted buffer is empty")
	}
	if aesKey == "" {
		return nil, errors.New("decrypt file: aes key is empty")
	}

	key, err := decodeBase64Lenient(aesKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt file: decode aes key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("decrypt file: aes key length = %d, want 32", len(key))
	}
	if len(encrypted)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("decrypt file: encrypted length %d is not AES block aligned", len(encrypted))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("decrypt file: create AES cipher: %w", err)
	}

	decrypted := make([]byte, len(encrypted))
	// 企业微信媒体解密算法与 Node SDK 一致：
	// AES-256-CBC，IV 取 key 前 16 字节，然后手动移除 32 字节块 PKCS#7 padding。
	mode := cipher.NewCBCDecrypter(block, key[:aes.BlockSize])
	mode.CryptBlocks(decrypted, encrypted)

	return pkcs7Unpad32(decrypted)
}

// decodeBase64Lenient decodes base64 the way Node's Buffer.from(s, "base64") does:
// it tolerates missing padding and accepts both the standard and URL-safe alphabets.
//
// WeCom media aeskeys are delivered as 43-char unpadded base64 (and may use URL-safe
// chars), which Go's strict base64.StdEncoding.DecodeString rejects (e.g. "illegal
// base64 data at input byte 40"). Normalizing the alphabet and re-padding fixes it.
func decodeBase64Lenient(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	// URL-safe alphabet → standard alphabet.
	s = strings.ReplaceAll(s, "-", "+")
	s = strings.ReplaceAll(s, "_", "/")
	// Re-pad to a multiple of 4 so StdEncoding accepts it.
	if m := len(s) % 4; m != 0 {
		s += strings.Repeat("=", 4-m)
	}
	return base64.StdEncoding.DecodeString(s)
}

// DownloadFile downloads media and decrypts it when aesKey is provided.
func DownloadFile(ctx context.Context, url string, aesKey string) (*DownloadedFile, error) {
	return DownloadFileWithClient(ctx, http.DefaultClient, url, aesKey)
}

// DownloadFileWithClient is DownloadFile with an injectable HTTP client for tests.
func DownloadFileWithClient(ctx context.Context, client *http.Client, url string, aesKey string) (*DownloadedFile, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("download file: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if aesKey != "" {
		body, err = DecryptFile(body, aesKey)
		if err != nil {
			return nil, err
		}
	}

	return &DownloadedFile{
		Buffer:   body,
		Filename: filenameFromContentDisposition(resp.Header.Get("Content-Disposition")),
	}, nil
}

func pkcs7Unpad32(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("pkcs7 unpad: empty buffer")
	}

	padLen := int(data[len(data)-1])
	if padLen < 1 || padLen > 32 || padLen > len(data) {
		return nil, fmt.Errorf("pkcs7 unpad: invalid padding length %d", padLen)
	}

	for i := len(data) - padLen; i < len(data); i++ {
		if int(data[i]) != padLen {
			return nil, errors.New("pkcs7 unpad: padding bytes mismatch")
		}
	}

	return data[:len(data)-padLen], nil
}

func filenameFromContentDisposition(header string) string {
	if header == "" {
		return ""
	}

	_, params, err := mime.ParseMediaType(header)
	if err != nil {
		return ""
	}
	name := params["filename"]
	// 企微常把 UTF-8 文件名按 percent-encoding 放进 filename=（如 "%E4%BC%81...png"）。
	// 若能成功 percent-decode 且结果是合法 UTF-8，用解码后的名字（还原中文等非 ASCII 原名）。
	// PathUnescape 对不含 % 的串原样返回、对非法转义返回 error（此时保留原值），故安全。
	if decoded, derr := url.PathUnescape(name); derr == nil && utf8.ValidString(decoded) {
		name = decoded
	}
	return name
}
