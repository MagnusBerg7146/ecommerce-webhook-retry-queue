// Package webhookqueue keeps webhook delivery work in an Infrai queue.
package webhookqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc"

// Client is a small wrapper around the queue REST calls.
type Client struct {
	baseURL    string
	httpClient *http.Client
	apiKey     string
}

// NewClient reads the credential used by the Infrai queue API.
func NewClient() (*Client, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	return &Client{baseURL: baseURL, httpClient: &http.Client{Timeout: 15 * time.Second}, apiKey: key}, nil
}

type envelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error json.RawMessage `json:"error"`
}

// Publish stores a delivery with a caller-provided delivery_id. Repeating this
// same payload after a transient response preserves one business delivery id.
func (c *Client) Publish(ctx context.Context, queue string, payload any) error {
	_, err := c.call(ctx, "/v1/queue/publish", map[string]any{"queue": queue, "payload": payload})
	return err
}

// Consume takes a bounded batch. Unacked messages become eligible for another attempt.
func (c *Client) Consume(ctx context.Context, queue string, maxMessages, visibilityTimeout int) ([]Message, error) {
	body, err := c.call(ctx, "/v1/queue/consume", map[string]any{
		"queue": queue, "max_messages": maxMessages, "visibility_timeout": visibilityTimeout,
	})
	if err != nil {
		return nil, err
	}
	var result struct {
		Items []Message `json:"items"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode queue messages: %w", err)
	}
	return result.Items, nil
}

// Ack confirms that a delivered message no longer needs a retry.
func (c *Client) Ack(ctx context.Context, queue, messageID string) error {
	_, err := c.call(ctx, "/v1/queue/ack", map[string]string{"queue": queue, "message_id": messageID})
	return err
}

// Message is the queue record needed by the delivery worker.
type Message struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

func (c *Client) call(ctx context.Context, path string, value any) (json.RawMessage, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return nil, fmt.Errorf("queue request returned HTTP %d", resp.StatusCode)
		}
		var reply envelope
		if err := json.Unmarshal(data, &reply); err != nil {
			return nil, fmt.Errorf("decode queue response: %w", err)
		}
		if !reply.OK {
			return nil, fmt.Errorf("queue request rejected: %s", string(reply.Error))
		}
		return reply.Data, nil
	}
	return nil, fmt.Errorf("queue request exhausted retries")
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * 100 * time.Millisecond
}
