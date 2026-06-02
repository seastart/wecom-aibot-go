package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadDotEnvFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	err := os.WriteFile(path, []byte(`
# comments should be ignored
WECOM_AIBOT_BOT_ID=bot-1
WECOM_AIBOT_SECRET="secret-1"
WECOM_AIBOT_ENDPOINT='wss://example.com/ws'
`), 0o600)
	if err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	values, err := readDotEnvFile(path)
	if err != nil {
		t.Fatalf("readDotEnvFile returned error: %v", err)
	}

	if values["WECOM_AIBOT_BOT_ID"] != "bot-1" {
		t.Fatalf("bot id = %q, want bot-1", values["WECOM_AIBOT_BOT_ID"])
	}
	if values["WECOM_AIBOT_SECRET"] != "secret-1" {
		t.Fatalf("secret = %q, want secret-1", values["WECOM_AIBOT_SECRET"])
	}
	if values["WECOM_AIBOT_ENDPOINT"] != "wss://example.com/ws" {
		t.Fatalf("endpoint = %q, want wss://example.com/ws", values["WECOM_AIBOT_ENDPOINT"])
	}
}

func TestMergeEnvKeepsRealEnvironmentFirst(t *testing.T) {
	t.Setenv("WECOM_AIBOT_BOT_ID", "real-env-bot")

	values := mergeEnv(map[string]string{
		"WECOM_AIBOT_BOT_ID": "file-bot",
		"WECOM_AIBOT_SECRET": "file-secret",
	})

	if values["WECOM_AIBOT_BOT_ID"] != "real-env-bot" {
		t.Fatalf("bot id = %q, want real-env-bot", values["WECOM_AIBOT_BOT_ID"])
	}
	if values["WECOM_AIBOT_SECRET"] != "file-secret" {
		t.Fatalf("secret = %q, want file-secret", values["WECOM_AIBOT_SECRET"])
	}
}
