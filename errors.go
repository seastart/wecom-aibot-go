package aibot

import (
	"fmt"
	"time"
)

// AckError is returned when WeCom replies with errcode != 0.
type AckError struct {
	Ack Ack
}

func (e AckError) Error() string {
	return fmt.Sprintf("wecom aibot ack error: req_id=%s errcode=%d errmsg=%s", e.Ack.ReqID, e.Ack.ErrCode, e.Ack.ErrMsg)
}

// ErrAckTimeout is returned when no ACK arrives for a sent frame.
type ErrAckTimeout struct {
	ReqID   string
	Timeout time.Duration
}

func (e ErrAckTimeout) Error() string {
	return fmt.Sprintf("wecom aibot ack timeout: req_id=%s timeout=%s", e.ReqID, e.Timeout)
}
