package aibot

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const uploadChunkSize = 512 * 1024

// UploadMediaOptions describes a temporary media upload.
type UploadMediaOptions struct {
	Type     MessageType
	Filename string
}

// UploadMediaResult is returned by aibot_upload_media_finish.
type UploadMediaResult struct {
	Type      MessageType `json:"type"`
	MediaID   string      `json:"media_id"`
	CreatedAt string      `json:"created_at"`
}

type uploadInitBody struct {
	Type        MessageType `json:"type"`
	Filename    string      `json:"filename"`
	TotalSize   int         `json:"total_size"`
	TotalChunks int         `json:"total_chunks"`
	MD5         string      `json:"md5"`
}

type uploadChunkBody struct {
	UploadID   string `json:"upload_id"`
	ChunkIndex int    `json:"chunk_index"`
	Base64Data string `json:"base64_data"`
}

type uploadFinishBody struct {
	UploadID string `json:"upload_id"`
}

// UploadMedia uploads temporary media through the long connection.
func (c *Client) UploadMedia(ctx context.Context, data []byte, options UploadMediaOptions) (*UploadMediaResult, error) {
	if len(data) == 0 {
		return nil, errors.New("upload media: data is empty")
	}
	if options.Type == "" || options.Filename == "" {
		return nil, errors.New("upload media: type and filename are required")
	}

	totalChunks := (len(data) + uploadChunkSize - 1) / uploadChunkSize
	if totalChunks > 100 {
		return nil, fmt.Errorf("upload media: %d chunks exceeds maximum 100", totalChunks)
	}

	hash := md5.Sum(data)
	initAck, err := c.SendAndWait(ctx, WsFrame[uploadInitBody]{
		Cmd:     WsCmdUploadMediaInit,
		Headers: WsHeaders{ReqID: NewReqID(WsCmdUploadMediaInit)},
		Body: uploadInitBody{
			Type:        options.Type,
			Filename:    options.Filename,
			TotalSize:   len(data),
			TotalChunks: totalChunks,
			MD5:         hex.EncodeToString(hash[:]),
		},
	})
	if err != nil {
		return nil, err
	}

	var initResult struct {
		UploadID string `json:"upload_id"`
	}
	if err := json.Unmarshal(initAck.Body, &initResult); err != nil {
		return nil, fmt.Errorf("upload media: parse init ack: %w", err)
	}
	if initResult.UploadID == "" {
		return nil, errors.New("upload media: init ack missing upload_id")
	}

	for idx := 0; idx < totalChunks; idx++ {
		start := idx * uploadChunkSize
		end := min(start+uploadChunkSize, len(data))
		_, err := c.SendAndWait(ctx, WsFrame[uploadChunkBody]{
			Cmd:     WsCmdUploadMediaChunk,
			Headers: WsHeaders{ReqID: NewReqID(WsCmdUploadMediaChunk)},
			Body: uploadChunkBody{
				UploadID:   initResult.UploadID,
				ChunkIndex: idx,
				Base64Data: base64.StdEncoding.EncodeToString(data[start:end]),
			},
		})
		if err != nil {
			return nil, err
		}
	}

	finishAck, err := c.SendAndWait(ctx, WsFrame[uploadFinishBody]{
		Cmd:     WsCmdUploadMediaFinish,
		Headers: WsHeaders{ReqID: NewReqID(WsCmdUploadMediaFinish)},
		Body:    uploadFinishBody{UploadID: initResult.UploadID},
	})
	if err != nil {
		return nil, err
	}

	var result UploadMediaResult
	if err := json.Unmarshal(finishAck.Body, &result); err != nil {
		return nil, fmt.Errorf("upload media: parse finish ack: %w", err)
	}
	if result.MediaID == "" {
		return nil, errors.New("upload media: finish ack missing media_id")
	}
	return &result, nil
}
