// retry-webhook publishes an order event, then delivers one queued batch.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	webhookqueue "example.com/ecommerce-webhook-retry"
)

type webhookEvent struct {
	DeliveryID string          `json:"delivery_id"`
	URL        string          `json:"url"`
	Body       json.RawMessage `json:"body"`
}

func main() {
	endpoint := os.Getenv("WEBHOOK_URL")
	if endpoint == "" {
		panic("WEBHOOK_URL is required")
	}
	queue := os.Getenv("WEBHOOK_QUEUE")
	if queue == "" {
		queue = "webhook-deliveries"
	}
	client, err := webhookqueue.NewClient()
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	event := webhookEvent{DeliveryID: "order-1042-paid", URL: endpoint, Body: json.RawMessage(`{"order_id":"1042","event":"order.paid"}`)}
	if err := client.Publish(ctx, queue, event); err != nil {
		panic(err)
	}
	messages, err := client.Consume(ctx, queue, 10, 30)
	if err != nil {
		panic(err)
	}
	for _, message := range messages {
		var queued webhookEvent
		if err := json.Unmarshal(message.Payload, &queued); err != nil {
			panic(err)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, queued.URL, bytes.NewReader(queued.Body))
		if err != nil {
			panic(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			continue
		}
		response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode > 299 {
			continue
		}
		if err := client.Ack(ctx, queue, message.MessageID); err != nil {
			panic(err)
		}
		fmt.Println("delivered", queued.DeliveryID)
	}
}
