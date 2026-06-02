package aibot

// WebSocket command names used by the WeCom intelligent robot protocol.
const (
	WsCmdSubscribe         = "aibot_subscribe"
	WsCmdHeartbeat         = "ping"
	WsCmdRespondMessage    = "aibot_respond_msg"
	WsCmdRespondWelcome    = "aibot_respond_welcome_msg"
	WsCmdRespondUpdate     = "aibot_respond_update_msg"
	WsCmdSendMessage       = "aibot_send_msg"
	WsCmdMessageCallback   = "aibot_msg_callback"
	WsCmdEventCallback     = "aibot_event_callback"
	WsCmdUploadMediaInit   = "aibot_upload_media_init"
	WsCmdUploadMediaChunk  = "aibot_upload_media_chunk"
	WsCmdUploadMediaFinish = "aibot_upload_media_finish"
)

// WsHeaders carries request correlation metadata.
type WsHeaders struct {
	ReqID string `json:"req_id"`
}

// WsFrame is the unified WebSocket frame shape used by the Node SDK and
// official long-connection protocol: {cmd, headers:{req_id}, body, errcode}.
type WsFrame[T any] struct {
	Cmd     string    `json:"cmd,omitempty"`
	Headers WsHeaders `json:"headers"`
	Body    T         `json:"body,omitempty"`
	ErrCode *int      `json:"errcode,omitempty"`
	ErrMsg  string    `json:"errmsg,omitempty"`
}

// GetHeaders allows generic frame aliases to be passed to SendAndWait.
func (f WsFrame[T]) GetHeaders() WsHeaders {
	return f.Headers
}
