package aibot

import (
	"encoding/json"
	"testing"
)

// TestTimestampAcceptsNumberAndString 锁死 Timestamp 的宽松契约：数字与字符串两种编码都要吃得下。
// 背景见 Timestamp 的类型说明（真机事故 2026-08-17：created_at 文档写字符串、真机返回数字）。
func TestTimestampAcceptsNumberAndString(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want Timestamp
	}{
		{"裸数字（真机形态）", `1380000000`, 1380000000},
		{"带引号数字（文档示例形态）", `"1380000000"`, 1380000000},
		{"null", `null`, 0},
		{"空字符串", `""`, 0},
		{"非数字占位不算错，降级为 0", `"now"`, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got Timestamp
			if err := json.Unmarshal([]byte(c.raw), &got); err != nil {
				t.Fatalf("Unmarshal(%s) returned error: %v", c.raw, err)
			}
			if got != c.want {
				t.Fatalf("Unmarshal(%s) = %d, want %d", c.raw, got, c.want)
			}
		})
	}
}

// TestTimestampNeverBreaksSiblingFields 是这次修复真正要防住的回归：
// 时间戳字段的编码形态漂移，绝不能连累同一个对象里的 media_id 取不到。
func TestTimestampNeverBreaksSiblingFields(t *testing.T) {
	for _, raw := range []string{
		`{"type":"image","media_id":"media-1","created_at":1380000000}`,
		`{"type":"image","media_id":"media-1","created_at":"1380000000"}`,
		`{"type":"image","media_id":"media-1","created_at":"now"}`,
	} {
		var res UploadMediaResult
		if err := json.Unmarshal([]byte(raw), &res); err != nil {
			t.Fatalf("Unmarshal(%s) returned error: %v", raw, err)
		}
		if res.MediaID != "media-1" {
			t.Fatalf("Unmarshal(%s): MediaID = %q, want media-1", raw, res.MediaID)
		}
	}
}

func TestTimestampMarshalsAsNumber(t *testing.T) {
	b, err := json.Marshal(struct {
		CreatedAt Timestamp `json:"created_at"`
	}{CreatedAt: 1380000000})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if string(b) != `{"created_at":1380000000}` {
		t.Fatalf("Marshal = %s, want {\"created_at\":1380000000}", b)
	}
}
