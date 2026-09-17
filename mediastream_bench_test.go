package aibot

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"io"
	"testing"
)

// 本文件量的是流式解密【唯一的存在理由】：内存占用与文件大小脱钩。
//
// ★ 口径刻意只覆盖解密层（bytes.Reader → io.Discard），**不经 httptest**：benchmark 里的
// httptest server 与 client 同进程，`w.Write(16MB)` 那一侧的分配会一并计进 B/op，把两者的
// 差距冲淡成「省了一半」——第一版就是这么测的，差点据此以为流式没做成。
// 要测一样东西，先把不属于它的分配挡在外面。
//
// 参考数字（Apple M1 Pro，16MB 明文）：
//
//	BenchmarkDecryptWhole16MB     36578342 B/op    41 allocs/op
//	BenchmarkDecryptStream16MB      355494 B/op    10 allocs/op   ← 换成 64MB 仍是这个数
func benchPayload(b *testing.B, size int) ([]byte, string) {
	b.Helper()
	key := bytes.Repeat([]byte{9}, 32)
	plain := bytes.Repeat([]byte("x"), size)
	block, err := aes.NewCipher(key)
	if err != nil {
		b.Fatal(err)
	}
	pad := 32 - len(plain)%32
	padded := append(append([]byte{}, plain...), bytes.Repeat([]byte{byte(pad)}, pad)...)
	enc := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(enc, padded)
	return enc, base64.StdEncoding.EncodeToString(key)
}

func BenchmarkDecryptWhole16MB(b *testing.B) {
	enc, key := benchPayload(b, 16<<20)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := DecryptFile(enc, key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecryptStream16MB(b *testing.B) {
	enc, key := benchPayload(b, 16<<20)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r, err := newDecryptReader(bytes.NewReader(enc), key)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, r); err != nil {
			b.Fatal(err)
		}
	}
}
