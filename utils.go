package aibot

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// NewReqID creates a protocol request id with a readable command prefix.
func NewReqID(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixMilli(), hex.EncodeToString(b[:]))
}

// Timestamp 是一个「宽松」的 Unix 秒级时间戳，同时吃得下 JSON 数字与字符串两种编码。
//
// 第一性原理：**协议里的装饰性字段，绝不能有权毁掉整条消息的解析**。
// 真机事故（2026-08-17）：aibot_upload_media_finish 的 created_at，官方文档响应示例写成带引号的
// "1380000000"，字段说明表却标 int，实际服务端返回裸数字——库按示例声明成 string，于是整个 finish ack
// 反序列化失败，media_id 明明就在包里却取不到，图片一张也发不出去。同类风险散布在所有腾讯系文档
// 「示例与说明表不一致」的字段上，故这里不去赌哪一种才对，两种都收。
//
// 解析不出来时【置 0 且不报错】，而不是返回 error：调用方要的是 media_id，
// 一个连自己都说不清是什么格式的时间戳，不配让主流程失败。
type Timestamp int64

// UnmarshalJSON 接受 1380000000 / "1380000000" / null / "" 四种形态，其余一律降级为 0。
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	s := strings.Trim(strings.TrimSpace(string(data)), `"`)
	if s == "" || s == "null" {
		*t = 0
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		*t = 0 // 未知形态（如文档里出现过的非数字占位）不算错，见类型说明
		return nil
	}
	*t = Timestamp(v)
	return nil
}

// MarshalJSON 统一按数字输出（与服务端真机行为一致）。
func (t Timestamp) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(t), 10)), nil
}

// Time 把时间戳转成 time.Time；零值返回零时间。
func (t Timestamp) Time() time.Time {
	if t == 0 {
		return time.Time{}
	}
	return time.Unix(int64(t), 0)
}
