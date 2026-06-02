package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	aibot "github.com/seastart/wecom-aibot-go"
)

func main() {
	cfg, err := loadConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := aibot.NewClient(cfg)

	client.OnAck(func(ctx context.Context, ack *aibot.Ack) error {
		log.Printf("ack req_id=%s errcode=%d errmsg=%s", ack.ReqID, ack.ErrCode, ack.ErrMsg)
		return nil
	})

	client.OnEvent(func(ctx context.Context, event *aibot.Event) error {
		logJSON("event", event)
		return nil
	})

	client.OnMessage(func(ctx context.Context, msg *aibot.Message) error {
		logJSON("message", msg)

		if msg.Quote != nil {
			logJSON("quote", msg.Quote)
		}

		// 核心测试逻辑：收到文本消息后立刻走 aibot_respond_msg 回复。
		// 普通回复在长连接协议里使用 msgtype=stream，NewTextReply 会构造
		// 一次性 finish=true 的 stream 回复。
		// 回复必须透传长连接帧 headers.req_id，不能使用 body.msgid。
		if msg.Type == aibot.MessageTypeText && msg.Text != nil {
			reply := fmt.Sprintf("收到：%s", msg.Text.Content)
			return client.Send(ctx, aibot.NewTextReply(msg.ReqID, reply))
		}

		return nil
	})

	log.Printf("connecting to %s", endpointForLog(cfg.Endpoint))
	if err := client.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func loadConfigFromEnv() (aibot.Config, error) {
	envFileValues, err := readDotEnvFile(filepath.Join("examples", "echo", ".env"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return aibot.Config{}, err
	}
	values := mergeEnv(envFileValues)

	botID := values["WECOM_AIBOT_BOT_ID"]
	secret := values["WECOM_AIBOT_SECRET"]
	if botID == "" || secret == "" {
		return aibot.Config{}, errors.New("missing WECOM_AIBOT_BOT_ID or WECOM_AIBOT_SECRET")
	}

	return aibot.Config{
		BotID:    botID,
		Secret:   secret,
		Endpoint: values["WECOM_AIBOT_ENDPOINT"],
	}, nil
}

func readDotEnvFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	values := make(map[string]string)
	for _, rawLine := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			continue
		}

		// 只处理最常见的 .env 写法：KEY=value、KEY="value"、KEY='value'。
		// 不做 shell 展开，避免凭证里出现特殊字符时被意外改写。
		value = strings.Trim(value, `"'`)
		values[key] = value
	}

	return values, nil
}

func mergeEnv(fileValues map[string]string) map[string]string {
	values := make(map[string]string, len(fileValues)+3)
	for key, value := range fileValues {
		values[key] = value
	}

	// shell 环境变量优先级高于 .env，方便临时覆盖某个值做调试。
	for _, key := range []string{"WECOM_AIBOT_BOT_ID", "WECOM_AIBOT_SECRET", "WECOM_AIBOT_ENDPOINT"} {
		if value := os.Getenv(key); value != "" {
			values[key] = value
		}
	}
	return values
}

func endpointForLog(endpoint string) string {
	if endpoint != "" {
		return endpoint
	}
	return "wss://openws.work.weixin.qq.com"
}

func logJSON(label string, value any) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Printf("%s: %+v", label, value)
		return
	}
	log.Printf("%s:\n%s", label, data)
}
