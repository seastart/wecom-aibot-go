# Node SDK 对齐情况

对比来源：`/Users/shushenghong/Documents/workspace/ai/skills/aibot-node-sdk`。

## 已对齐

| 能力 | Node SDK | Go 当前状态 |
| --- | --- | --- |
| WebSocket 地址 | `wss://openws.work.weixin.qq.com` | 已对齐 |
| 帧格式 | `{ cmd, headers: { req_id }, body, errcode, errmsg }` | 已对齐 |
| 订阅命令 | `aibot_subscribe`，body 为 `bot_id/secret` | 已对齐 |
| 心跳命令 | `ping` | 已对齐 |
| 消息回调 | `aibot_msg_callback`，解析 `body` | 已对齐 |
| 事件回调 | `aibot_event_callback`，解析 `body.event.eventtype` | 已对齐 |
| ACK | 无 `cmd`，通过 `headers.req_id` 关联 | 基础解析已对齐 |
| 收到消息类型 | `text/image/mixed/voice/file/video` | 结构体已覆盖 |
| 引用消息 | `quote?: QuoteContent` | 结构体已覆盖 |
| 下载并解密媒体 | HTTP 下载 + AES-256-CBC 解密 | 已对齐，`DownloadFile` / `DecryptFile` |
| 一次性文本回复 | Node 通过 `msgtype=stream + finish=true` 表达 | 已对齐，`NewTextReply` |
| 多段流式回复 | `replyStream(frame, streamID, content, finish)` | 已对齐，`NewStreamReply` |
| Ctrl+C 退出 | 断开 WebSocket | 已支持 context cancel 关闭连接 |
| 回复 ACK 队列 | 同一个 `req_id` 串行发送，等待 ACK 后再发下一条 | 已对齐，`SendAndWait` |
| ACK 超时 | 5s 超时 reject | 已对齐，`ReplyAckTimeout` |
| 重连 | 指数退避重连 | 已对齐，`RunForever` |
| 心跳健康 | missed pong 超阈值断开 | 已对齐，`MaxMissedPong` |
| 主动发送消息 | markdown/template_card/media | 已补 markdown/template_card/media，`NewTextPush` 标记 deprecated |
| 欢迎语回复 | `aibot_respond_welcome_msg` | 已对齐，`NewWelcomeTextReply` |
| 模板卡片回复 | `template_card` | 已支持宽松 `TemplateCard` |
| 更新模板卡片 | `aibot_respond_update_msg` | 已对齐，`NewUpdateTemplateCard` |
| 媒体回复/主动媒体 | `replyMedia/sendMediaMessage` | 已对齐，`NewMediaReply` / `NewMediaPush` |
| 上传临时素材 | init/chunk/finish 三段上传 | 已对齐，`UploadMedia` |
| 短连接加解密 | WeCom crypto | 已支持 `NewWecomCrypto`，并在 `WebhookHandler` 中接入加密 URL 验证、消息解密和加密回复 |
| `disconnected_event` | 收到新连接踢下线事件后停止重连 | 已对齐，收到后停止 `RunForever` 重连 |

## 重要差异

| 能力 | Node SDK | Go 当前状态 | 风险 |
| --- | --- | --- | --- |
| 流式 + 卡片 | `stream_with_template_card` | 未实现 | 复杂回复不可用 |
| 事件细分 | `enter_chat/template_card_event/feedback/disconnected_event` | 只有通用 `EventContent` | 类型提示不足 |
| 日志接口 | 可注入 logger | 未实现 | 生产排障不够细 |

## 建议优先级

1. **流式 + 卡片**：补 `stream_with_template_card`。
2. **事件细分类型**：补 `enter_chat/template_card_event/feedback/disconnected_event` 强类型。
3. **日志接口**：补可注入 logger，便于生产排障。

## 当前使用建议

- 测试收发文本：用 `examples/echo`。
- 一次性回复：`NewTextReply(msg.ReqID, content)`。
- 流式回复：多次调用 `NewStreamReply(msg.ReqID, streamID, content, finish)`。
- 常驻 gateway：使用 `RunForever(ctx)`。
