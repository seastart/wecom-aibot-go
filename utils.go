package aibot

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// NewReqID creates a protocol request id with a readable command prefix.
func NewReqID(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixMilli(), hex.EncodeToString(b[:]))
}
