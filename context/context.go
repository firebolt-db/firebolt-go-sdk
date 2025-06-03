package context

import (
	"context"
)

type ContextKey string

type PreparedStatementsStyle int

const (
	PreparedStatementsStyleNative PreparedStatementsStyle = iota
	PreparedStatementsStyleFbNumeric
)

const AdditionalHeadersContextKey = ContextKey("additionalHeaders")
const StreamingContextKey = ContextKey("streaming")
const PreparedStatementsStyleContextKey = ContextKey("preparedStatementsStyle")

func WithStreaming(ctx context.Context) context.Context {
	return context.WithValue(ctx, StreamingContextKey, true)
}

func WithAdditionalHeaders(ctx context.Context, headers map[string]string) context.Context {
	return context.WithValue(ctx, AdditionalHeadersContextKey, headers)
}

func WithPreparedStatementsStyle(ctx context.Context, style PreparedStatementsStyle) context.Context {
	return context.WithValue(ctx, PreparedStatementsStyleContextKey, style)
}

func IsStreaming(ctx context.Context) bool {
	IsStreaming, ok := ctx.Value(StreamingContextKey).(bool)
	return ok && IsStreaming
}

func GetAdditionalHeaders(ctx context.Context) (map[string]string, bool) {
	headers, ok := ctx.Value(AdditionalHeadersContextKey).(map[string]string)
	return headers, ok
}

func GetPreparedStatementsStyle(ctx context.Context) PreparedStatementsStyle {
	style, ok := ctx.Value(PreparedStatementsStyleContextKey).(PreparedStatementsStyle)
	if !ok {
		return PreparedStatementsStyleNative
	}
	return style
}
