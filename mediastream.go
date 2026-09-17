package aibot

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// MediaStream is an open media download whose bytes are already decrypted.
//
// 第一性原理：媒体可以很大（企业微信允许 100M 以内的文件回调），而 DownloadFile 会把
// 密文与明文各在内存里放一整份——一个 100M 的附件峰值就是 200M 起，且接收方往往只是
// 想把它写到磁盘上。流式接口让内存占用与文件大小无关（一个 chunk + 32 字节尾巴）。
//
// 形态上刻意返回 Reader 而不是接收 Writer：下游通常是「给我名字和 reader，我来落盘」
// 这类 pull 模型（os.Create + io.Copy 也是），返回 Reader 两边自然对接；若接收 Writer，
// 调用方要么改自己的落盘函数签名、要么靠 io.Pipe 转接（多一个 goroutine 与错误传播）。
type MediaStream struct {
	// Filename is parsed from the Content-Disposition header (may be empty).
	Filename string
	// ReadCloser yields the decrypted plaintext; closing it closes the HTTP body.
	io.ReadCloser
}

// OpenFile starts a streaming download, decrypting on the fly when aesKey is provided.
// The caller must Close the returned stream.
func OpenFile(ctx context.Context, url string, aesKey string) (*MediaStream, error) {
	return OpenFileWithClient(ctx, http.DefaultClient, url, aesKey)
}

// OpenFileWithClient is OpenFile with an injectable HTTP client.
func OpenFileWithClient(ctx context.Context, client *http.Client, url string, aesKey string) (*MediaStream, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("download file: HTTP %d", resp.StatusCode)
	}

	body := io.ReadCloser(resp.Body)
	if aesKey != "" {
		dec, err := newDecryptReader(resp.Body, aesKey)
		if err != nil {
			resp.Body.Close()
			return nil, err
		}
		// 关掉解密层时要一并关掉 HTTP body，否则连接泄漏。
		body = &readCloser{Reader: dec, closer: resp.Body}
	}

	return &MediaStream{
		Filename:   filenameFromContentDisposition(resp.Header.Get("Content-Disposition")),
		ReadCloser: body,
	}, nil
}

// readCloser 把一个 Reader 与另一个 Closer 缝在一起（解密层本身没有底层资源要释放）。
type readCloser struct {
	io.Reader
	closer io.Closer
}

func (rc *readCloser) Close() error { return rc.closer.Close() }

// maxPadLen 是 PKCS#7 填充可能占用的最大字节数。
//
// ★ 企业微信这套用的是 **32 字节块**的 PKCS#7（见 pkcs7Unpad32），而 AES 的分组是 16——
// 两个数不一样，这是最容易看走眼的一处。流式解密必须按【填充的上限】保留尾巴，按 16 保留
// 会在 padLen>16 时把填充字节当正文吐出去。
const maxPadLen = 32

// decryptStreamChunk 是每次从上游读取的密文块大小（必须是 AES 分组的整数倍，且足够大以摊薄
// 系统调用开销）。它直接决定流式路径的内存占用上限。
const decryptStreamChunk = 64 * 1024

// decryptReader 把「AES-256-CBC + 32 字节块 PKCS#7」的密文流解成明文流。
//
// 两级缓冲，两个都是坑：
//
//	① 密文侧 carry —— 上游（网络）的读边界【不对齐】AES 分组，一次 Read 可能给 1000 字节，
//	   而 CryptBlocks 要求输入是 16 的整数倍。故留下不足一组的残余，拼到下一轮前面。
//	   到达 EOF 时 carry 必须为空，否则密文长度就不是分组对齐的（与 DecryptFile 的校验等价）。
//
//	② 明文侧 hold —— PKCS#7 的填充在【最后】，而流式时无从知道哪一块是最后一块。故永远扣住
//	   尾部 maxPadLen 字节不输出，读到 EOF 才对它去填充。扣少了就会把填充当正文吐出去。
//
// CBC 的 IV 链由 cipher.BlockMode 自己保着，分批调用 CryptBlocks 是安全的。
type decryptReader struct {
	src   io.Reader
	mode  cipher.BlockMode
	buf   []byte       // 从上游读取的暂存区
	plain []byte       // 解密输出的暂存区
	carry []byte       // 未对齐的密文残余（< aes.BlockSize）
	hold  []byte       // 已解密但暂不输出的明文尾巴（<= maxPadLen）
	out   bytes.Buffer // 待输出的明文
	eof   bool         // 上游已读完且尾巴已处理
	err   error        // 粘滞错误
}

func newDecryptReader(src io.Reader, aesKey string) (*decryptReader, error) {
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
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("decrypt file: create AES cipher: %w", err)
	}
	// ★ 四个缓冲全部【预分配到单轮上限】，此后 append 永不扩容。
	// 这不是微优化：carry / hold 若从零 cap 起步，每一轮 append 都会重新分配一个 chunk，
	// 于是「流式」的累计分配仍与文件大小同阶（实测 16MB 文件分配 35MB，与整份读相差无几），
	// 流式就只剩下一半好处。单轮能拿到的密文最多是「chunk + 上一轮残余(<分组)」，明文同阶，
	// 再加上要扣住的那条尾巴，三个上限由此而来。
	const round = decryptStreamChunk + aes.BlockSize
	return &decryptReader{
		src:   src,
		mode:  cipher.NewCBCDecrypter(block, key[:aes.BlockSize]),
		buf:   make([]byte, decryptStreamChunk),
		plain: make([]byte, round),
		carry: make([]byte, 0, round),
		hold:  make([]byte, 0, round+maxPadLen),
	}, nil
}

func (r *decryptReader) Read(p []byte) (int, error) {
	for r.out.Len() == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if r.eof {
			return 0, io.EOF
		}
		r.fill()
	}
	return r.out.Read(p)
}

// fill 从上游取一段密文，解密后把「除尾巴之外」的明文放进 out。
func (r *decryptReader) fill() {
	n, rerr := r.src.Read(r.buf)
	if n > 0 {
		data := append(r.carry, r.buf[:n]...)
		usable := len(data) - len(data)%aes.BlockSize
		r.carry = append(r.carry[:0], data[usable:]...)
		if usable > 0 {
			plain := r.plain[:usable]
			r.mode.CryptBlocks(plain, data[:usable])
			all := append(r.hold, plain...)
			if len(all) > maxPadLen {
				cut := len(all) - maxPadLen
				r.out.Write(all[:cut])
				r.hold = append(r.hold[:0], all[cut:]...)
			} else {
				r.hold = all
			}
		}
	}

	switch {
	case rerr == io.EOF:
		r.finish()
	case rerr != nil:
		r.err = rerr
	}
}

// finish 在上游读完时收尾：校验密文对齐、对扣住的尾巴去填充。
func (r *decryptReader) finish() {
	if len(r.carry) != 0 {
		r.err = fmt.Errorf("decrypt file: encrypted length is not AES block aligned (%d trailing bytes)", len(r.carry))
		return
	}
	tail, err := pkcs7Unpad32(r.hold)
	if err != nil {
		r.err = err
		return
	}
	r.out.Write(tail)
	r.hold = nil
	r.eof = true
}
