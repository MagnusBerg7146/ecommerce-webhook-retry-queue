package webhookqueue

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (fn roundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

func TestPublishRetriesAndKeepsDeliveryID(t *testing.T) {
	calls := 0
	client := &Client{baseURL: "https://queue.test", apiKey: "test", httpClient: &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
		calls++
		body, _ := io.ReadAll(req.Body)
		if req.Method != http.MethodPost || !strings.Contains(string(body), "delivery-7") {
			t.Fatal("publish request lost its delivery id")
		}
		if calls == 1 {
			return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": []string{"0"}}, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"data":{},"error":null,"metadata":{}}`))}, nil
	})}}
	if err := client.Publish(context.Background(), "webhook-deliveries", map[string]string{"delivery_id": "delivery-7"}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}
