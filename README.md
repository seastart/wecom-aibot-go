# wecom-aibot-go

企微智能机器人 Go 库，面向 gateway 场景：

- 官方文档：[智能机器人长连接](https://developer.work.weixin.qq.com/document/path/101463)
- 官方文档：[回调和回复的加解密方案](https://developer.work.weixin.qq.com/document/path/101033)
- 长连接：连接 `wss://openws.work.weixin.qq.com`，订阅机器人，接收实时消息/事件，发送回复/主动推送。
- 短连接：提供 webhook handler，完成明文/加密模式下的签名校验、URL 验证、消息解析和 JSON 回复。
- 消息结构体：显式建模文本、图片、图文混排、语音、文件、视频、引用消息字段，同时保留原始 JSON。

本库的长连接帧格式已按官方 Node SDK 对齐：

```json
{
  "cmd": "aibot_msg_callback",
  "headers": { "req_id": "REQ_ID" },
  "body": {
    "msgid": "MSG_ID",
    "msgtype": "text"
  }
}
```

常用命令包括：

| 方向 | 命令 | 说明 |
| --- | --- | --- |
| 开发者 -> 企微 | `aibot_subscribe` | 认证订阅 |
| 开发者 -> 企微 | `ping` | 心跳 |
| 开发者 -> 企微 | `aibot_respond_msg` | 回复消息，普通文本内容使用 `msgtype=stream` |
| 开发者 -> 企微 | `aibot_send_msg` | 主动发送消息 |
| 开发者 -> 企微 | `aibot_upload_media_init/chunk/finish` | 上传临时素材 |
| 企微 -> 开发者 | `aibot_msg_callback` | 消息推送回调 |
| 企微 -> 开发者 | `aibot_event_callback` | 事件推送回调 |

收到的 `Message` 包含 `Quote *QuoteContent` 字段。用户引用其他消息时，企业微信会在 `quote` 中放入被引用内容，当前支持 `text/image/mixed/voice/file`。

## 长连接示例

仓库内置了一个可直接测试的 echo 示例：

```bash
cp examples/echo/.env.example examples/echo/.env
```

然后编辑 `examples/echo/.env`：

```env
WECOM_AIBOT_BOT_ID=你的 BotID
WECOM_AIBOT_SECRET=你的长连接 Secret
WECOM_AIBOT_ENDPOINT=
```

启动：

```bash
go run ./examples/echo
```

如果企业管理端给的是私有部署长连接地址，可以额外指定：

```env
WECOM_AIBOT_ENDPOINT=wss://你的长连接地址
```

`examples/echo/.env` 已加入 `.gitignore`，不会提交到仓库。shell 里的同名环境变量优先级高于 `.env`，方便临时覆盖调试。

示例会打印收到的消息、引用消息、事件和 ACK；收到文本消息时，会用回调帧的 `headers.req_id` 发送 `aibot_respond_msg` 回复。长连接普通回复按 Node SDK 对齐为 `msgtype=stream`，不是 `msgtype=text`。

### 回复方式

一次性回复，也就是业务上的“非流式”：

```go
client.Send(ctx, aibot.NewTextReply(msg.ReqID, "收到："+msg.Text.Content))
```

底层仍然会按企业微信长连接协议发送：

```json
{
  "cmd": "aibot_respond_msg",
  "headers": { "req_id": "回调帧里的 req_id" },
  "body": {
    "msgtype": "stream",
    "stream": {
      "id": "stream_xxx",
      "finish": true,
      "content": "收到：hello"
    }
  }
}
```

多段流式回复：

```go
streamID := aibot.NewReqID("stream")

_ = client.Send(ctx, aibot.NewStreamReply(msg.ReqID, streamID, "正在处理...", false))
_ = client.Send(ctx, aibot.NewStreamReply(msg.ReqID, streamID, "处理完成", true))
```

等待 ACK 的发送方式：

```go
ack, err := client.SendAndWait(ctx, aibot.NewStreamReply(msg.ReqID, streamID, "处理完成", true))
_ = ack
_ = err
```

`SendAndWait` 会按 `headers.req_id` 串行发送并等待企微 ACK，适合流式多段回复。

常驻运行建议：

```go
if err := client.RunForever(ctx); err != nil {
    log.Fatal(err)
}
```

媒体下载解密：

```go
file, err := aibot.DownloadFile(ctx, msg.Image.URL, msg.Image.AESKey)
_ = file
_ = err
```

上传临时素材并回复图片：

```go
result, err := client.UploadMedia(ctx, imageBytes, aibot.UploadMediaOptions{
    Type:     aibot.MessageTypeImage,
    Filename: "image.png",
})
if err == nil {
    _ = client.SendAndWait(ctx, aibot.NewMediaReply(msg.ReqID, aibot.MessageTypeImage, result.MediaID, nil))
}
```

```go
package main

import (
	"context"
	"log"
	"os"

	aibot "github.com/seastart/wecom-aibot-go"
)

func main() {
	client := aibot.NewClient(aibot.Config{
		BotID:  os.Getenv("WECOM_AIBOT_BOT_ID"),
		Secret: os.Getenv("WECOM_AIBOT_SECRET"),
	})

	client.OnMessage(func(ctx context.Context, msg *aibot.Message) error {
		if msg.Type == aibot.MessageTypeText && msg.Text != nil {
			return client.Send(ctx, aibot.NewTextReply(msg.ReqID, "收到："+msg.Text.Content))
		}
		return nil
	})

	if err := client.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

## 主动推送

除了"收到消息再回复"，还可以用 `aibot_send_msg` **主动**给某个会话推送消息（不依赖回调 `req_id`）。库提供了 Markdown / 模板卡片 / 媒体三种便捷构造函数，第二个参数 `chatType` 为必填，建议用 `SendAndWait` 发送以拿到企微 ACK：

```go
// 群聊推送：chatid 用群聊回调里的 chatid
push := aibot.NewMarkdownPush(chatID, aibot.ChatTypeGroup, "**发布完成**\n结果：成功")
if _, err := client.SendAndWait(ctx, push); err != nil {
    log.Printf("push failed: %v", err)
}
```

> ⚠️ **`chatType` 必须正确指定，否则单聊寻址会失败。** 它告诉服务端如何解析 `chatid`：
> - `aibot.ChatTypeSingle`（1）= 单聊，此时 `chatid` 传**用户的 userid**；
> - `aibot.ChatTypeGroup`（2）= 群聊，此时 `chatid` 传群聊回调里的 `chatid`。
>
> 若缺省（0），服务端会**优先按群聊解析**，单聊会发不出去——所以库把 `chatType` 设计成了构造函数的必填形参，避免遗漏。

模板卡片和媒体推送同理：

```go
// 模板卡片推送（单聊：chatid 传 userid）
card := aibot.TemplateCard{ /* 官方模板卡片 JSON 结构 */ }
cardPush := aibot.NewTemplateCardPush(userID, aibot.ChatTypeSingle, card)

// 媒体推送（先 UploadMedia 拿到 media_id）
mediaPush := aibot.NewMediaPush(chatID, aibot.ChatTypeGroup, aibot.MessageTypeImage, mediaID, nil)

_, _ = client.SendAndWait(ctx, cardPush)
_, _ = client.SendAndWait(ctx, mediaPush)
```

> 频率限制：无论回复还是主动推送，单会话合计 **30 条/分钟、1000 条/小时**。

## 事件回调

除了消息回调（`aibot_msg_callback`），企业微信还会通过 `aibot_event_callback` 推送**事件**。长连接模式下共支持 4 种事件，用 `client.OnEvent` 注册处理器接收：

| eventtype | 含义 | 回复要求 | 对应能力 |
| --- | --- | --- | --- |
| `enter_chat` | 用户当天首次进入机器人单聊会话 | 尽快回复（约 5 秒内），否则当天再次进入不再推送 | `NewWelcomeTextReply` 回欢迎语 |
| `template_card_event` | 用户点击模板卡片的按钮/选项 | **必须 5 秒内响应**，否则连接被断开 | `NewUpdateTemplateCard` 更新卡片 |
| `feedback_event` | 用户对回复点赞/点踩 | 仅支持回空包，不能发新消息或更新卡片 | 读取 `Feedback` 字段 |
| `disconnected_event` | 有新连接建立，服务端主动断开旧连接 | 无需回复 | 库内部据此**停止自动重连** |

> 官方文档：[事件回调](https://developer.work.weixin.qq.com/document/path/101027)、[长连接·接收事件回调](https://developer.work.weixin.qq.com/document/path/101463)

事件的业务字段**嵌套在与 `eventtype` 同名的子对象里**，库已按官方 schema 建模：`template_card_event` 的详情在 `event.Event.TemplateCard`（含 `EventKey` / `TaskID` / `CardType` / `SelectedItems`），`feedback_event` 的详情在 `event.Event.Feedback`（含 `ID` / `Type` / `Content` / `InaccurateReasonList`）。

```go
client.OnEvent(func(ctx context.Context, event *aibot.Event) error {
    if event.Event == nil {
        return nil
    }
    // 回复事件同样要透传事件帧的 headers.req_id（event.ReqID），不能用 body.msgid。
    switch event.Event.EventType {
    case aibot.EventTypeEnterChat:
        return client.Send(ctx, aibot.NewWelcomeTextReply(event.ReqID, "你好，有什么可以帮你？"))
    case aibot.EventTypeTemplateCard:
        card := event.Event.TemplateCard // 用户点了哪个按钮：card.EventKey
        _ = card
        return nil
    case aibot.EventTypeFeedback:
        fb := event.Event.Feedback // fb.Type：1=准确 2=不准确 3=取消
        _ = fb
        return nil
    case aibot.EventTypeDisconnected:
        // 连接被新实例抢占，RunForever 会停止重连，避免互相踢下线。
        return nil
    }
    return nil
})
```

> ⚠️ 收到 `disconnected_event` 说明该机器人的长连接被另一个实例抢占了（同一机器人同时只能有一个有效长连接）。此时 `RunForever` 会**故意不再重连**。如果线上出现"机器人突然不回消息"，先排查是不是有多个实例在连同一个 BotID。

## 短连接示例

```go
package main

import (
	"context"
	"net/http"
	"os"

	aibot "github.com/seastart/wecom-aibot-go"
)

func main() {
	handler := aibot.NewWebhookHandler(
		aibot.WebhookConfig{
			Token:          os.Getenv("WECOM_AIBOT_TOKEN"),
			EncodingAESKey: os.Getenv("WECOM_AIBOT_ENCODING_AES_KEY"),
			// 企业内部智能机器人按官方回调加解密文档使用空 ReceiveID。
			ReceiveID: "",
		},
		func(ctx context.Context, msg *aibot.Message) (*aibot.WebhookResponse, error) {
			return aibot.NewWebhookTextResponse("收到"), nil
		},
	)

	http.Handle("/wecom/callback", handler)
	_ = http.ListenAndServe(":8080", nil)
}
```

## 重要限制

同一个企业微信智能机器人同一时间只能保持一个有效长连接。多项目复用时，建议只让一个 `wecom-aibot-go` gateway 连接企业微信，再由 gateway 分发消息到其他业务项目。

短连接 handler 支持明文 JSON 回调，也支持加密 JSON 回调：GET 地址验证会解密 `echostr` 并返回明文，POST 会对 `encrypt` 字段验签、解密后解析消息，业务响应会重新加密为 `encrypt/msgsignature/timestamp/nonce`。
