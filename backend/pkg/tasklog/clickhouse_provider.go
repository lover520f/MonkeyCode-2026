package tasklog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/chaitin/MonkeyCode/backend/pkg/clickhouse"
)

type ClickHouseProvider struct {
	client *clickhouse.Client
}

func NewClickHouseProvider(client *clickhouse.Client) *ClickHouseProvider {
	return &ClickHouseProvider{client: client}
}

func (p *ClickHouseProvider) Name() string {
	return "clickhouse"
}

func (p *ClickHouseProvider) QueryWindow(ctx context.Context, taskID uuid.UUID, start, end time.Time) ([]Entry, error) {
	if p.client == nil {
		return nil, ErrProviderUnavailable
	}

	const q = `
SELECT task_id, ts, event, kind, turn_seq, data, msg_seq
FROM task_logs
WHERE task_id = ? AND ts >= ? AND ts <= ?
ORDER BY ts ASC, ingest_id ASC`

	rows, err := p.client.QueryContext(ctx, q, taskID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]Entry, 0)
	for rows.Next() {
		var (
			id      string
			ts      time.Time
			event   string
			kind    string
			turnSeq uint32
			data    string
			msgSeq  string
		)
		if err := rows.Scan(&id, &ts, &event, &kind, &turnSeq, &data, &msgSeq); err != nil {
			return nil, err
		}
		parsedID, err := uuid.Parse(id)
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{
			TaskID:  parsedID,
			TS:      ts.UTC(),
			Event:   event,
			Kind:    kind,
			TurnSeq: turnSeq,
			Data:    data,
			MsgSeq:  msgSeq,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (p *ClickHouseProvider) FindLatestTurnStart(ctx context.Context, taskID uuid.UUID, taskCreatedAt, end time.Time) (time.Time, error) {
	if p.client == nil {
		return time.Time{}, ErrProviderUnavailable
	}

	const q = `
SELECT min(ts)
FROM task_logs
WHERE task_id = ? AND ts >= ? AND ts <= ? AND turn_seq = (
	SELECT max(turn_seq)
	FROM task_logs
	WHERE task_id = ? AND ts >= ? AND ts <= ?
)`

	var ts sql.NullTime
	if err := p.client.QueryRowContext(ctx, q, taskID, taskCreatedAt, end, taskID, taskCreatedAt, end).Scan(&ts); err != nil {
		return time.Time{}, err
	}
	if !ts.Valid {
		return taskCreatedAt, nil
	}
	return ts.Time.UTC(), nil
}

func (p *ClickHouseProvider) QueryTurns(ctx context.Context, taskID uuid.UUID, start, end time.Time, limit int) (*QueryTurnsResp, error) {
	if p.client == nil {
		return nil, ErrProviderUnavailable
	}
	if limit <= 0 {
		limit = 2
	}
	if limit > 10 {
		limit = 10
	}

	const qTurns = `
SELECT turn_seq, min(ts) AS turn_start
FROM task_logs
WHERE task_id = ? AND ts >= ? AND ts <= ?
GROUP BY turn_seq
ORDER BY turn_seq DESC
LIMIT ?`

	rows, err := p.client.QueryContext(ctx, qTurns, taskID, start, end, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type turnMeta struct {
		Seq   uint32
		Start time.Time
	}

	turns := make([]turnMeta, 0, limit+1)
	for rows.Next() {
		var meta turnMeta
		if err := rows.Scan(&meta.Seq, &meta.Start); err != nil {
			return nil, err
		}
		turns = append(turns, meta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(turns) == 0 {
		return &QueryTurnsResp{}, nil
	}

	hasMore := len(turns) > limit
	if hasMore {
		turns = turns[:limit]
	}

	placeholders := make([]string, 0, len(turns))
	args := make([]any, 0, len(turns)+3)
	args = append(args, taskID)
	for _, meta := range turns {
		placeholders = append(placeholders, "?")
		args = append(args, meta.Seq)
	}
	args = append(args, start, end)

	qChunks := fmt.Sprintf(`
SELECT ts, event, kind, data
FROM task_logs
WHERE task_id = ? AND turn_seq IN (%s) AND ts >= ? AND ts <= ?
ORDER BY turn_seq DESC, ts ASC, ingest_id ASC`, strings.Join(placeholders, ", "))

	chunkRows, err := p.client.QueryContext(ctx, qChunks, args...)
	if err != nil {
		return nil, err
	}
	defer chunkRows.Close()

	resp := &QueryTurnsResp{
		Chunks:  make([]*RoundChunk, 0),
		HasMore: hasMore,
	}
	if hasMore {
		resp.NextTS = turns[len(turns)-1].Start.Add(-time.Nanosecond).UnixNano()
	}

	for chunkRows.Next() {
		var (
			ts    time.Time
			event string
			kind  string
			data  string
		)
		if err := chunkRows.Scan(&ts, &event, &kind, &data); err != nil {
			return nil, err
		}
		resp.Chunks = append(resp.Chunks, &RoundChunk{
			Data:      []byte(data),
			Event:     event,
			Kind:      kind,
			Timestamp: ts.UTC().UnixNano(),
		})
	}
	if err := chunkRows.Err(); err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *ClickHouseProvider) QueryUserInputs(ctx context.Context, taskIDs []uuid.UUID, end time.Time, limit int) ([]Entry, error) {
	if p.client == nil {
		return nil, ErrProviderUnavailable
	}
	if limit <= 0 {
		limit = 20
	}

	query := `
SELECT task_id, ts, event, kind, turn_seq, data, msg_seq
FROM task_logs
WHERE ts <= ? AND event IN ('user-input', 'reply-question')`
	args := make([]any, 0, len(taskIDs)+2)
	args = append(args, end)
	if len(taskIDs) > 0 {
		holders := make([]string, 0, len(taskIDs))
		for _, taskID := range taskIDs {
			holders = append(holders, "?")
			args = append(args, taskID)
		}
		query += " AND task_id IN (" + strings.Join(holders, ", ") + ")"
	}
	query += " ORDER BY ts DESC LIMIT ?"
	args = append(args, limit)

	rows, err := p.client.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]Entry, 0, limit)
	for rows.Next() {
		var (
			id      string
			ts      time.Time
			event   string
			kind    string
			turnSeq uint32
			data    string
			msgSeq  string
		)
		if err := rows.Scan(&id, &ts, &event, &kind, &turnSeq, &data, &msgSeq); err != nil {
			return nil, err
		}
		parsedID, err := uuid.Parse(id)
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{
			TaskID:  parsedID,
			TS:      ts.UTC(),
			Event:   event,
			Kind:    kind,
			TurnSeq: turnSeq,
			Data:    data,
			MsgSeq:  msgSeq,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (p *ClickHouseProvider) CountEvents(ctx context.Context, taskIDs []uuid.UUID, events []string) (int, error) {
	if p.client == nil {
		return 0, ErrProviderUnavailable
	}
	query := "SELECT count() FROM task_logs WHERE 1=1"
	args := make([]any, 0, len(taskIDs)+len(events))
	if len(taskIDs) > 0 {
		holders := make([]string, 0, len(taskIDs))
		for _, taskID := range taskIDs {
			holders = append(holders, "?")
			args = append(args, taskID)
		}
		query += " AND task_id IN (" + strings.Join(holders, ", ") + ")"
	}
	if len(events) > 0 {
		holders := make([]string, 0, len(events))
		for _, event := range events {
			holders = append(holders, "?")
			args = append(args, event)
		}
		query += " AND event IN (" + strings.Join(holders, ", ") + ")"
	}

	var count int
	if err := p.client.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
