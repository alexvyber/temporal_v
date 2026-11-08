package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type Handle struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}
type contextKey struct{}
type Journal struct {
	mutex sync.Mutex
	path  string
}

func (journal *Journal) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	file, err := os.OpenFile(journal.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, span := range spans {
		row := map[string]string{"name": span.Name(), "trace_id": span.SpanContext().TraceID().String(), "span_id": span.SpanContext().SpanID().String(), "parent_span_id": span.Parent().SpanID().String()}
		if err = json.NewEncoder(file).Encode(row); err != nil {
			return err
		}
	}
	return nil
}
func (*Journal) Shutdown(context.Context) error { return nil }

var workflowTracer interceptor.Tracer

func Interop(ctx workflow.Context) (string, error) {
	if !workflow.IsReplaying(ctx) {
		span, ok := ctx.Value(contextKey{}).(interceptor.TracerSpan)
		if !ok {
			return "", fmt.Errorf("Go workflow tracer span missing")
		}
		carrier, err := workflowTracer.MarshalSpan(span)
		if err != nil {
			return "", err
		}
		if !strings.Contains(carrier["traceparent"], os.Getenv("V_TRACE_ID")) {
			return "", fmt.Errorf("Go workflow trace identity changed")
		}
		bag, err := baggage.Parse(carrier["baggage"])
		if err != nil || bag.Member("tenant").Value() != "interop" {
			return "", fmt.Errorf("Go workflow baggage missing")
		}
	}
	stage := 0
	if err := workflow.SetQueryHandler(ctx, "state", func() (int, error) { return stage, nil }); err != nil {
		return "", err
	}
	workflow.GetSignalChannel(ctx, "ready").Receive(ctx, nil)
	stage++
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	return "interop", nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	mode := os.Args[1]
	logger := log.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if mode == "replay" {
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(Interop, workflow.RegisterOptions{Name: "v.trace.interop"})
		if err := replayer.ReplayWorkflowHistoryFromJSONFile(logger, os.Args[2]); err != nil {
			return err
		}
		fmt.Println("OK")
		return nil
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(&Journal{path: os.Getenv("V_TRACE_GO_JOURNAL")}))
	defer provider.Shutdown(context.Background())
	tracer, err := temporalotel.NewTracer(temporalotel.TracerOptions{Tracer: provider.Tracer("temporal-v-go-reference"), SpanContextKey: contextKey{}})
	if err != nil {
		return err
	}
	workflowTracer = tracer
	tracing := interceptor.NewTracingInterceptor(tracer)
	connection, err := client.Dial(client.Options{HostPort: strings.TrimPrefix(os.Getenv("TEMPORAL_ADDRESS"), "http://"), Logger: logger, Interceptors: []interceptor.ClientInterceptor{tracing}})
	if err != nil {
		return err
	}
	defer connection.Close()
	if mode == "worker" {
		instance := worker.New(connection, os.Getenv("TEMPORAL_TASK_QUEUE"), worker.Options{Interceptors: []interceptor.WorkerInterceptor{tracing}})
		instance.RegisterWorkflowWithOptions(Interop, workflow.RegisterOptions{Name: "v.trace.interop"})
		return instance.Run(worker.InterruptCh())
	}
	id, err := trace.TraceIDFromHex(os.Getenv("V_TRACE_ID"))
	if err != nil {
		return err
	}
	parent, err := trace.SpanIDFromHex("abcdef1234567890")
	if err != nil {
		return err
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: id, SpanID: parent, TraceFlags: trace.FlagsSampled}))
	bag, err := baggage.Parse("tenant=interop")
	if err != nil {
		return err
	}
	ctx = baggage.ContextWithBaggage(ctx, bag)
	if mode == "start" {
		execution, err := connection.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: os.Getenv("TEMPORAL_TASK_QUEUE") + "-interop", TaskQueue: os.Getenv("TEMPORAL_TASK_QUEUE")}, "v.trace.interop")
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(Handle{execution.GetID(), execution.GetRunID()})
	}
	var handle Handle
	if err = json.Unmarshal([]byte(os.Args[2]), &handle); err != nil {
		return err
	}
	switch mode {
	case "ready", "finish":
		err = connection.SignalWorkflow(ctx, handle.WorkflowID, handle.RunID, mode, nil)
	case "state":
		var response int
		encoded, e := connection.QueryWorkflow(ctx, handle.WorkflowID, handle.RunID, "state")
		if e != nil {
			return e
		}
		if e = encoded.Get(&response); e != nil {
			return e
		}
		fmt.Println(response)
		return nil
	case "result":
		var value string
		err = connection.GetWorkflow(ctx, handle.WorkflowID, handle.RunID).Get(ctx, &value)
		if err == nil {
			if value != "interop" {
				return fmt.Errorf("unexpected result")
			}
			fmt.Println("OK")
			return nil
		}
	case "reset-sticky":
		_, err = connection.WorkflowService().ResetStickyTaskQueue(ctx, &workflowservice.ResetStickyTaskQueueRequest{Namespace: "default", Execution: &commonpb.WorkflowExecution{WorkflowId: handle.WorkflowID, RunId: handle.RunID}})
	default:
		return fmt.Errorf("unknown reference mode")
	}
	if err != nil {
		return err
	}
	fmt.Println("OK")
	return nil
}
