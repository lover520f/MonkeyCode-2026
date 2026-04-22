package taskflow

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestCreateVirtualMachineReqIncludesLogStore(t *testing.T) {
	req := CreateVirtualMachineReq{
		UserID:   "u-1",
		HostID:   "h-1",
		TaskID:   uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		LogStore: "clickhouse",
	}

	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"log_store":"clickhouse"`) {
		t.Fatalf("marshaled request missing log_store: %s", string(b))
	}
}

func TestRestartTaskReqIncludesLogStore(t *testing.T) {
	req := RestartTaskReq{
		ID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		RequestId: "req-1",
		LogStore:  "clickhouse",
	}

	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"log_store":"clickhouse"`) {
		t.Fatalf("marshaled request missing log_store: %s", string(b))
	}
}

func TestAskUserQuestionResponseIncludesLogStore(t *testing.T) {
	req := AskUserQuestionResponse{
		TaskId:      "task-1",
		RequestId:   "req-1",
		AnswersJson: `{"ok":true}`,
		LogStore:    "clickhouse",
	}

	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"log_store":"clickhouse"`) {
		t.Fatalf("marshaled request missing log_store: %s", string(b))
	}
}

func TestTaskReqIncludesLogStore(t *testing.T) {
	req := TaskReq{
		Task: &Task{
			ID:       uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			LogStore: "clickhouse",
		},
	}

	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"log_store":"clickhouse"`) {
		t.Fatalf("marshaled request missing log_store: %s", string(b))
	}
}
