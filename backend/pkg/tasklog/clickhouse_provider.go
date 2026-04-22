package tasklog

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
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

func (p *ClickHouseProvider) QueryLatestTurn(ctx context.Context, taskID uuid.UUID, taskCreatedAt, end time.Time) (*QueryLatestTurnResp, error) {
	if p.client == nil {
		return nil, ErrProviderUnavailable
	}

	const qTurn = `
SELECT max(turn_seq)
FROM task_logs
WHERE task_id = ? AND ts >= ? AND ts <= ?`

	var latestTurn sql.NullInt64
	if err := p.client.QueryRowContext(ctx, qTurn, taskID, taskCreatedAt, end).Scan(&latestTurn); err != nil {
		return nil, err
	}
	if !latestTurn.Valid || latestTurn.Int64 <= 0 {
		return &QueryLatestTurnResp{}, nil
	}

	entries, err := p.queryEntriesByTurn(ctx, taskID, uint32(latestTurn.Int64), taskCreatedAt, end)
	if err != nil {
		return nil, err
	}

	resp := &QueryLatestTurnResp{
		Entries: entries,
		HasMore: latestTurn.Int64 > 1,
	}
	if resp.HasMore {
		resp.NextCursor = strconv.FormatInt(latestTurn.Int64-1, 10)
	}
	return resp, nil
}

func (p *ClickHouseProvider) QueryTurns(ctx context.Context, taskID uuid.UUID, _ time.Time, cursor string, limit int) (*QueryTurnsResp, error) {
	if p.client == nil {
		return nil, ErrProviderUnavailable
	}
	if limit <= 0 {
		limit = 2
	}
	if limit > 10 {
		limit = 10
	}

	upperTurn, err := p.resolveUpperTurn(ctx, taskID, cursor)
	if err != nil {
		return nil, err
	}
	if upperTurn == 0 {
		return &QueryTurnsResp{}, nil
	}

	const qTurns = `
SELECT turn_seq
FROM task_logs
WHERE task_id = ? AND turn_seq <= ?
GROUP BY turn_seq
ORDER BY turn_seq DESC
LIMIT ?`

	rows, err := p.client.QueryContext(ctx, qTurns, taskID, upperTurn, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	turns := make([]uint32, 0, limit+1)
	for rows.Next() {
		var seq uint32
		if err := rows.Scan(&seq); err != nil {
			return nil, err
		}
		turns = append(turns, seq)
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

	chunks, err := p.queryTurnChunks(ctx, taskID, turns)
	if err != nil {
		return nil, err
	}

	resp := &QueryTurnsResp{
		Chunks:  chunks,
		HasMore: hasMore,
	}
	if hasMore {
		oldest := turns[len(turns)-1]
		if oldest > 1 {
			resp.NextCursor = strconv.FormatUint(uint64(oldest-1), 10)
		}
	}
	return resp, nil
}

func (p *ClickHouseProvider) resolveUpperTurn(ctx context.Context, taskID uuid.UUID, cursor string) (uint32, error) {
	if cursor != "" {
		n, err := strconv.ParseUint(cursor, 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(n), nil
	}

	const q = `SELECT max(turn_seq) FROM task_logs WHERE task_id = ?`
	var latest sql.NullInt64
	if err := p.client.QueryRowContext(ctx, q, taskID).Scan(&latest); err != nil {
		return 0, err
	}
	if !latest.Valid || latest.Int64 <= 0 {
		return 0, nil
	}
	return uint32(latest.Int64), nil
}

func (p *ClickHouseProvider) queryEntriesByTurn(ctx context.Context, taskID uuid.UUID, turnSeq uint32, start, end time.Time) ([]Entry, error) {
	const q = `
SELECT task_id, ts, event, kind, turn_seq, data, msg_seq
FROM task_logs
WHERE task_id = ? AND turn_seq = ? AND ts >= ? AND ts <= ?
ORDER BY ts ASC, ingest_id ASC`

	rows, err := p.client.QueryContext(ctx, q, taskID, turnSeq, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]Entry, 0)
	for rows.Next() {
		var (
			id     string
			ts     time.Time
			event  string
			kind   string
			seq    uint32
			data   string
			msgSeq string
		)
		if err := rows.Scan(&id, &ts, &event, &kind, &seq, &data, &msgSeq); err != nil {
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
			TurnSeq: seq,
			Data:    data,
			MsgSeq:  msgSeq,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (p *ClickHouseProvider) queryTurnChunks(ctx context.Context, taskID uuid.UUID, turns []uint32) ([]*TurnChunk, error) {
	placeholders := make([]string, 0, len(turns))
	args := make([]any, 0, len(turns)+1)
	args = append(args, taskID)
	for _, turn := range turns {
		placeholders = append(placeholders, "?")
		args = append(args, turn)
	}

	q := fmt.Sprintf(`
SELECT ts, event, kind, data
FROM task_logs
WHERE task_id = ? AND turn_seq IN (%s)
ORDER BY turn_seq DESC, ts ASC, ingest_id ASC`, strings.Join(placeholders, ", "))

	rows, err := p.client.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chunks := make([]*TurnChunk, 0)
	for rows.Next() {
		var (
			ts    time.Time
			event string
			kind  string
			data  string
		)
		if err := rows.Scan(&ts, &event, &kind, &data); err != nil {
			return nil, err
		}
		chunks = append(chunks, &TurnChunk{
			Data:      []byte(data),
			Event:     event,
			Kind:      kind,
			Timestamp: ts.UTC().UnixNano(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}
