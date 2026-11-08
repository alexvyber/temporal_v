package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type Pending struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	ProtocolID string `json:"protocol_instance_id"`
	Policy     string `json:"unfinished_policy"`
}

func Lifecycle(ctx workflow.Context, input string) (string, error) {
	if input == "next" {
		return "continued", nil
	}
	mode := ""
	released := false
	pending := map[string]Pending{}
	if err := workflow.SetQueryHandler(ctx, "state", func() (any, error) {
		values := make([]Pending, 0, len(pending))
		for _, value := range pending {
			values = append(values, value)
		}
		sort.Slice(values, func(first, second int) bool { return values[first].ID < values[second].ID })
		return struct {
			Pending  []Pending `json:"pending"`
			Finished bool      `json:"all_finished"`
		}{values, workflow.AllHandlersFinished(ctx)}, nil
	}); err != nil {
		return "", err
	}
	for _, name := range []string{"hold-warn", "hold-silent", "immediate", "close"} {
		policy := workflow.HandlerUnfinishedPolicyWarnAndAbandon
		textPolicy := "warn_and_abandon"
		if name == "hold-silent" {
			policy = workflow.HandlerUnfinishedPolicyAbandon
			textPolicy = "abandon"
		}
		if err := workflow.SetUpdateHandlerWithOptions(ctx, name, func(updateContext workflow.Context, ignored any) (string, error) {
			if name == "immediate" {
				return "immediate", nil
			}
			id := workflow.GetCurrentUpdateInfo(updateContext).ID
			pending[id] = Pending{id, name, id, textPolicy}
			if name == "close" {
				mode = "handler-close"
			}
			if err := workflow.Await(updateContext, func() bool { return released }); err != nil {
				return "", err
			}
			delete(pending, id)
			return id, nil
		}, workflow.UpdateHandlerOptions{UnfinishedPolicy: policy}); err != nil {
			return "", err
		}
	}
	for _, name := range []string{"complete", "continue", "cancel", "fail", "finish-updates"} {
		channel := workflow.GetSignalChannel(ctx, name)
		workflow.Go(ctx, func(signalContext workflow.Context) {
			for {
				var ignored any
				channel.Receive(signalContext, &ignored)
				if name == "finish-updates" {
					released = true
				} else {
					mode = name
				}
			}
		})
	}
	if err := workflow.Await(ctx, func() bool { return mode != "" }); err != nil {
		return "", err
	}
	switch mode {
	case "continue":
		return "", workflow.NewContinueAsNewError(ctx, "v.update-lifecycle", "next")
	case "cancel":
		return "", temporal.NewCanceledError()
	case "fail":
		return "", temporal.NewNonRetryableApplicationError("lifecycle failure", "VApplicationError", nil)
	case "handler-close":
		return "handler-closed", nil
	default:
		return "closed", nil
	}
}

func run() error {
	logger := log.NewStructuredLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if os.Args[1] == "replay" {
		replay := worker.NewWorkflowReplayer()
		replay.RegisterWorkflowWithOptions(Lifecycle, workflow.RegisterOptions{Name: "v.update-lifecycle"})
		if err := replay.ReplayWorkflowHistoryFromJSONFile(logger, os.Args[2]); err != nil {
			return err
		}
		fmt.Println("OK")
		return nil
	}
	connection, err := client.Dial(client.Options{HostPort: strings.TrimPrefix(os.Getenv("TEMPORAL_ADDRESS"), "http://"), Logger: logger})
	if err != nil {
		return err
	}
	defer connection.Close()
	if os.Args[1] == "reset-sticky" {
		var handle struct {
			WorkflowID string `json:"workflow_id"`
			RunID      string `json:"run_id"`
		}
		if err := json.Unmarshal([]byte(os.Args[2]), &handle); err != nil {
			return err
		}
		_, err := connection.WorkflowService().ResetStickyTaskQueue(context.Background(), &workflowservice.ResetStickyTaskQueueRequest{Namespace: "default", Execution: &commonpb.WorkflowExecution{WorkflowId: handle.WorkflowID, RunId: handle.RunID}})
		return err
	}
	instance := worker.New(connection, os.Getenv("TEMPORAL_TASK_QUEUE"), worker.Options{})
	instance.RegisterWorkflowWithOptions(Lifecycle, workflow.RegisterOptions{Name: "v.update-lifecycle"})
	return instance.Run(worker.InterruptCh())
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
