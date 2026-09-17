package aibot

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
)

// TestDecryptReaderRoundTripAcrossReadShapes 是本文件最要紧的一条：**上游的读边界不对齐 AES
// 分组**，而这正是流式解密唯一真正容易错的地方——错了之后只在特定文件长度上显形。
//
// 故同一份密文用三种读法各跑一遍：正常读、每次 1 字节（把 carry 逻辑逼到极限）、每次半份。
// 长度取在分组(16)与填充块(32)的边界附近，外加一个跨多轮 chunk 的大块。
func TestDecryptReaderRoundTripAcrossReadShapes(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	aesKey := base64.StdEncoding.EncodeToString(key)

	lengths := []int{0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 1024, decryptStreamChunk + 123}
	shapes := map[string]func(io.Reader) io.Reader{
		"plain":   func(r io.Reader) io.Reader { return r },
		"oneByte": func(r io.Reader) io.Reader { return iotest.OneByteReader(r) },
		"half":    func(r io.Reader) io.Reader { return iotest.HalfReader(r) },
	}

	for _, n := range lengths {
		plain := make([]byte, n)
		if _, err := rand.Read(plain); err != nil {
			t.Fatalf("rand: %v", err)
		}
		encrypted := encryptForTest(t, plain, key)

		for name, shape := range shapes {
			t.Run(fmt.Sprintf("len=%d/%s", n, name), func(t *testing.T) {
				r, err := newDecryptReader(shape(bytes.NewReader(encrypted)), aesKey)
				if err != nil {
					t.Fatalf("newDecryptReader: %v", err)
				}
				got, err := io.ReadAll(r)
				if err != nil {
					t.Fatalf("ReadAll: %v", err)
				}
				if !bytes.Equal(got, plain) {
					t.Fatalf("解出来的明文不对: got %d bytes, want %d bytes", len(got), len(plain))
				}
			})
		}
	}
}

// TestDecryptFileMatchesStream 整份解与流式解必须逐字节一致——它们共用同一个解密器，
// 这条用例守的是「将来谁也别再写第二份实现」。
func TestDecryptFileMatchesStream(t *testing.T) {
	key := bytes.Repeat([]byte{3}, 32)
	aesKey := base64.StdEncoding.EncodeToString(key)
	plain := bytes.Repeat([]byte("企业微信媒体流式解密 "), 5000) // 远大于一个 chunk
	encrypted := encryptForTest(t, plain, key)

	whole, err := DecryptFile(encrypted, aesKey)
	if err != nil {
		t.Fatalf("DecryptFile: %v", err)
	}

	r, err := newDecryptReader(iotest.HalfReader(bytes.NewReader(encrypted)), aesKey)
	if err != nil {
		t.Fatalf("newDecryptReader: %v", err)
	}
	streamed, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if !bytes.Equal(whole, streamed) {
		t.Fatal("整份解与流式解结果不一致")
	}
	if !bytes.Equal(whole, plain) {
		t.Fatal("解出来的明文与原文不一致")
	}
}

// TestDecryptReaderRejectsUnalignedCiphertext 密文长度不是分组对齐时必须报错，
// 绝不能静默把残余字节丢掉（那会悄悄产出一份截断的文件）。
func TestDecryptReaderRejectsUnalignedCiphertext(t *testing.T) {
	key := bytes.Repeat([]byte{5}, 32)
	aesKey := base64.StdEncoding.EncodeToString(key)
	encrypted := encryptForTest(t, []byte("hello"), key)

	r, err := newDecryptReader(bytes.NewReader(encrypted[:len(encrypted)-3]), aesKey)
	if err != nil {
		t.Fatalf("newDecryptReader: %v", err)
	}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("密文长度不对齐时应报错")
	}
}

// TestDecryptReaderRejectsBadPadding 坏填充要在流末尾被抓到（与 DecryptFile 等价）。
func TestDecryptReaderRejectsBadPadding(t *testing.T) {
	key := bytes.Repeat([]byte{5}, 32)
	aesKey := base64.StdEncoding.EncodeToString(key)
	encrypted := encryptForTest(t, []byte("hello"), key)
	encrypted[len(encrypted)-1] = 99

	r, err := newDecryptReader(bytes.NewReader(encrypted), aesKey)
	if err != nil {
		t.Fatalf("newDecryptReader: %v", err)
	}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("填充损坏时应报错")
	}
}

// TestOpenFileStreamsAndDecrypts OpenFile 的端到端：解密 + 文件名 + Close 关掉 HTTP body。
func TestOpenFileStreamsAndDecrypts(t *testing.T) {
	key := bytes.Repeat([]byte{9}, 32)
	plain := bytes.Repeat([]byte("streamed media payload "), 10000)
	encrypted := encryptForTest(t, plain, key)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Disposition", `attachment; filename="%E8%B4%B5%E5%B7%9E.zip"`)
		w.Write(encrypted)
	}))
	defer srv.Close()

	stream, err := OpenFileWithClient(context.Background(), srv.Client(), srv.URL,
		base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatalf("OpenFileWithClient: %v", err)
	}
	defer stream.Close()

	if stream.Filename != "贵州.zip" {
		t.Errorf("Filename = %q，想要 %q", stream.Filename, "贵州.zip")
	}
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("内容不一致: got %d bytes, want %d bytes", len(got), len(plain))
	}
}

// TestOpenFileWithoutKeyPassesThrough 不给密钥时原样透传（语音等未加密场景）。
func TestOpenFileWithoutKeyPassesThrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "raw bytes")
	}))
	defer srv.Close()

	stream, err := OpenFileWithClient(context.Background(), srv.Client(), srv.URL, "")
	if err != nil {
		t.Fatalf("OpenFileWithClient: %v", err)
	}
	defer stream.Close()

	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "raw bytes" {
		t.Fatalf("got %q", got)
	}
}

// TestOpenFileRejectsHTTPError 非 2xx 要当场失败，且不能把响应体泄漏成内容。
func TestOpenFileRejectsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := OpenFileWithClient(context.Background(), srv.Client(), srv.URL, "")
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("应报 HTTP 403，实际: %v", err)
	}
}
