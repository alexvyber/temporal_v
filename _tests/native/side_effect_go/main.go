package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go.temporal.io/sdk/log"
	"log/slog"
	"os"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type Input struct {
	GoShape bool `json:"go_shape"`
}
type Configuration struct {
	Stage   string `json:"stage"`
	Padding string `json:"padding"`
}
type State struct {
	Tokens        []string `json:"tokens"`
	Configuration string   `json:"configuration"`
	Phase         int      `json:"phase"`
}
type Handle struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

func recordedRandom(ctx workflow.Context) string {
	var value []byte
	err := workflow.SideEffect(ctx, func(workflow.Context) interface{} {
		value := make([]byte, 32)
		if _, err := rand.Read(value); err != nil {
			panic(err)
		}
		return value
	}).Get(&value)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(value)
}
func settings(ctx workflow.Context) string {
	var value Configuration
	err := workflow.MutableSideEffect(ctx, "configuration", func(workflow.Context) interface{} {
		data, err := os.ReadFile(os.Getenv("V_EFFECT_CONFIG"))
		if err != nil {
			panic(err)
		}
		return Configuration{Stage: string(data), Padding: "small"}
	}, func(previous, current interface{}) bool {
		return previous.(Configuration).Stage == current.(Configuration).Stage
	}).Get(&value)
	if err != nil {
		panic(err)
	}
	return value.Stage
}
func Effect(ctx workflow.Context, input Input) (State, error) {
	state := State{Tokens: []string{recordedRandom(ctx)}}
	state.Configuration = settings(ctx)
	state.Configuration = settings(ctx)
	state.Configuration = settings(ctx)
	state.Tokens = append(state.Tokens, recordedRandom(ctx))
	state.Phase = 1
	if err := workflow.SetQueryHandler(ctx, "state", func() (State, error) { return state, nil }); err != nil {
		return state, err
	}
	workflow.GetSignalChannel(ctx, "change").Receive(ctx, nil)
	state.Configuration = settings(ctx)
	state.Configuration = settings(ctx)
	state.Tokens = append(state.Tokens, recordedRandom(ctx))
	state.Phase = 2
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	return state, nil
}
func main() {
	mode := os.Args[1]
	if mode == "replay" {
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(Effect, workflow.RegisterOptions{Name: "v.side-effect"})
		if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, os.Args[2]); err != nil {
			panic(err)
		}
		fmt.Println("OK")
		return
	}
	connection, err := client.Dial(client.Options{HostPort: strings.TrimPrefix(os.Getenv("TEMPORAL_ADDRESS"), "http://"), Logger: log.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, nil)))})
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	queue := os.Getenv("TEMPORAL_TASK_QUEUE")
	if mode == "reset-sticky" {
		var handle Handle
		if err = json.Unmarshal([]byte(os.Args[2]), &handle); err != nil {
			panic(err)
		}
		_, err = connection.WorkflowService().ResetStickyTaskQueue(context.Background(), &workflowservice.ResetStickyTaskQueueRequest{Namespace: "default", Execution: &commonpb.WorkflowExecution{WorkflowId: handle.WorkflowID, RunId: handle.RunID}})
		if err != nil {
			panic(err)
		}
		fmt.Println("OK")
		return
	}
	if mode == "worker" {
		instance := worker.New(connection, queue, worker.Options{})
		instance.RegisterWorkflowWithOptions(Effect, workflow.RegisterOptions{Name: "v.side-effect"})
		if err = instance.Run(worker.InterruptCh()); err != nil {
			panic(err)
		}
		return
	}
	if mode == "start" {
		run, err := connection.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: queue + "-go", TaskQueue: queue}, "v.side-effect", Input{GoShape: true})
		if err != nil {
			panic(err)
		}
		if err = json.NewEncoder(os.Stdout).Encode(Handle{run.GetID(), run.GetRunID()}); err != nil {
			panic(err)
		}
		return
	}
	panic("unknown oracle mode")
}
