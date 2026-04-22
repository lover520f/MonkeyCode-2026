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

func TestClickHouseProviderQueryLatestTurnUsesTurnSeqCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	taskID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	start := time.Unix(1_710_000_000, 0).UTC()
	end := start.Add(time.Minute)

	mock.ExpectQuery("SELECT max\\(turn_seq\\)").
		WithArgs(taskID, start, end).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(2))

	rows := sqlmock.NewRows([]string{"task_id", "ts", "event", "kind", "turn_seq", "data", "msg_seq"}).
		AddRow(taskID.String(), start.Add(10*time.Second), "user-input", "", 2, "hello", "1").
		AddRow(taskID.String(), start.Add(11*time.Second), "task-running", "acp_event", 2, `{"text":"world"}`, "2")

	mock.ExpectQuery("SELECT task_id, ts, event, kind, turn_seq, data, msg_seq").
		WithArgs(taskID, 2, start, end).
		WillReturnRows(rows)

	provider := tasklog.NewClickHouseProvider(clickhouse.NewWithDB(db))
	resp, err := provider.QueryLatestTurn(context.Background(), taskID, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(resp.Entries))
	}
	if !resp.HasMore {
		t.Fatal("expected has_more=true")
	}
	if resp.NextCursor != "1" {
		t.Fatalf("next_cursor = %q, want 1", resp.NextCursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestClickHouseProviderQueryTurnsUsesTurnSeqCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	taskID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	rows := sqlmock.NewRows([]string{"turn_seq"}).
		AddRow(2).
		AddRow(1)

	mock.ExpectQuery("SELECT turn_seq").
		WithArgs(taskID, uint32(2), 2).
		WillReturnRows(rows)

	chunkRows := sqlmock.NewRows([]string{"ts", "event", "kind", "data"}).
		AddRow(time.Unix(1_710_000_010, 0).UTC(), "user-input", "", "latest")

	mock.ExpectQuery("SELECT ts, event, kind, data").
		WithArgs(taskID, uint32(2)).
		WillReturnRows(chunkRows)

	provider := tasklog.NewClickHouseProvider(clickhouse.NewWithDB(db))
	resp, err := provider.QueryTurns(context.Background(), taskID, time.Time{}, "2", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Chunks) != 1 {
		t.Fatalf("len(chunks) = %d, want 1", len(resp.Chunks))
	}
	if !resp.HasMore {
		t.Fatal("expected has_more=true")
	}
	if resp.NextCursor != "1" {
		t.Fatalf("next_cursor = %q, want 1", resp.NextCursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
