package tasklog_test

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"github.com/chaitin/MonkeyCode/backend/pkg/clickhouse"
	"github.com/chaitin/MonkeyCode/backend/pkg/tasklog"
)

func TestClickHouseProviderQueryWindow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	taskID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	start := time.Unix(1_710_000_000, 0).UTC()
	end := start.Add(time.Minute)
	rows := sqlmock.NewRows([]string{"task_id", "ts", "event", "kind", "turn_seq", "data", "msg_seq"}).
		AddRow(taskID.String(), start, "user-input", "", 1, "hello", "1").
		AddRow(taskID.String(), start.Add(time.Second), "task-running", "acp_event", 1, `{"text":"world"}`, "2")

	mock.ExpectQuery("SELECT task_id, ts, event, kind, turn_seq, data, msg_seq").
		WithArgs(taskID, start, end).
		WillReturnRows(rows)

	provider := tasklog.NewClickHouseProvider(clickhouse.NewWithDB(db))
	entries, err := provider.QueryWindow(context.Background(), taskID, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}
	if entries[1].Kind != "acp_event" {
		t.Fatalf("kind = %q, want acp_event", entries[1].Kind)
	}
	if entries[1].TurnSeq != 1 {
		t.Fatalf("turn_seq = %d, want 1", entries[1].TurnSeq)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
