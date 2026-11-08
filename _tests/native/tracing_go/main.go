package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"os"
)

type Case struct {
	Parent string `json:"parent"`
	State  string `json:"state"`
}
type Context struct {
	TraceID    string `json:"trace_id"`
	SpanID     string `json:"span_id"`
	TraceFlags byte   `json:"trace_flags"`
	TraceState string `json:"trace_state"`
	Remote     bool   `json:"remote"`
}
type Result struct {
	Context Context           `json:"context"`
	Headers map[string]string `json:"headers"`
}

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var cases []Case
	if err = json.Unmarshal(data, &cases); err != nil {
		panic(err)
	}
	propagator := propagation.TraceContext{}
	for _, input := range cases {
		ctx := propagator.Extract(context.Background(), propagation.MapCarrier{"traceparent": input.Parent, "tracestate": input.State})
		value := trace.SpanContextFromContext(ctx)
		if !value.IsValid() {
			fmt.Println("0")
			continue
		}
		headers := propagation.MapCarrier{}
		propagator.Inject(ctx, headers)
		output := Result{Context{value.TraceID().String(), value.SpanID().String(), byte(value.TraceFlags()), value.TraceState().String(), value.IsRemote()}, headers}
		encoded, err := json.Marshal(output)
		if err != nil {
			panic(err)
		}
		fmt.Println("1|" + string(encoded))
	}
}
