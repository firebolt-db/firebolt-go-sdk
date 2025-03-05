package context

import (
	"context"
)

type ContextKey string

const AdditionalHeadersContextKey = ContextKey("additionalHeaders")
const StreamingContextKey = ContextKey("streaming")

func WithStreaming(ctx context.Context) context.Context {
	return context.WithValue(ctx, StreamingContextKey, true)
}

func WithAdditionalHeaders(ctx context.Context, headers map[string]string) context.Context {
	return context.WithValue(ctx, AdditionalHeadersContextKey, headers)
}

func IsStreaming(ctx context.Context) bool {
	IsStreaming, ok := ctx.Value(StreamingContextKey).(bool)
	return ok && IsStreaming
}

func GetAdditionalHeaders(ctx context.Context) (map[string]string, bool) {
	headers, ok := ctx.Value(AdditionalHeadersContextKey).(map[string]string)
	return headers, ok
}
