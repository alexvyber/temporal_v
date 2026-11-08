package main

import (
	"context"
	"encoding/json"
	"fmt"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"os"
	"strconv"
)

type Case struct {
	Method   string `json:"method"`
	Fraction string `json:"fraction"`
	TraceID  string `json:"trace_id"`
	Parent   bool   `json:"parent"`
	Remote   bool   `json:"remote"`
	Flags    uint8  `json:"flags"`
	State    string `json:"state"`
}
type Result struct {
	Decision int    `json:"decision"`
	State    string `json:"state"`
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
	for _, input := range cases {
		fraction, err := strconv.ParseFloat(input.Fraction, 64)
		if err != nil {
			panic(err)
		}
		id, err := trace.TraceIDFromHex(input.TraceID)
		if err != nil {
			panic(err)
		}
		ctx := context.Background()
		if input.Parent {
			spanID, err := trace.SpanIDFromHex("abcdef1234567890")
			if err != nil {
				panic(err)
			}
			state, err := trace.ParseTraceState(input.State)
			if err != nil {
				panic(err)
			}
			ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: id, SpanID: spanID, TraceFlags: trace.TraceFlags(input.Flags), TraceState: state, Remote: input.Remote}))
		}
		var sampler sdktrace.Sampler
		switch input.Method {
		case "ratio":
			sampler = sdktrace.TraceIDRatioBased(fraction)
		case "parent":
			sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(fraction))
		case "on":
			sampler = sdktrace.AlwaysSample()
		case "off":
			sampler = sdktrace.NeverSample()
		default:
			panic("invalid sampling reference strategy")
		}
		result := sampler.ShouldSample(sdktrace.SamplingParameters{ParentContext: ctx, TraceID: id, Name: "fixture", Kind: trace.SpanKindInternal})
		output, err := json.Marshal(Result{int(result.Decision), result.Tracestate.String()})
		if err != nil {
			panic(err)
		}
		fmt.Println(string(output))
	}
}
