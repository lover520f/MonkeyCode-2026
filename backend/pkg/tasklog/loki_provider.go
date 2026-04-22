package tasklog

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/chaitin/MonkeyCode/backend/pkg/loki"
	"github.com/chaitin/MonkeyCode/backend/pkg/taskflow"
)

type LokiProvider struct {
	client *loki.Client
}

func NewLokiProvider(client *loki.Client) *LokiProvider {
	return &LokiProvider{client: client}
}

func (p *LokiProvider) Name() string {
	return "loki"
}

func (p *LokiProvider) QueryWindow(ctx context.Context, taskID uuid.UUID, start, end time.Time) ([]Entry, error) {
	if p.client == nil {
		return nil, ErrProviderUnavailable
	}
	entries, err := p.client.QueryWindowByTaskID(ctx, taskID.String(), start, end)
	if err != nil {
		return nil, err
	}

	out := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		var chunk taskflow.TaskChunk
		if err := json.Unmarshal([]byte(entry.Line), &chunk); err != nil {
			continue
		}
		out = append(out, Entry{
			TaskID: taskID,
			TS:     entry.Timestamp.UTC(),
			Event:  chunk.Event,
			Kind:   chunk.Kind,
			Data:   string(chunk.Data),
			MsgSeq: entry.Labels["msg_seq"],
			Labels: entry.Labels,
		})
	}
	return out, nil
}

func (p *LokiProvider) FindLatestTurnStart(ctx context.Context, taskID uuid.UUID, taskCreatedAt, end time.Time) (time.Time, error) {
	if p.client == nil {
		return time.Time{}, ErrProviderUnavailable
	}
	return p.client.FindLatestRoundStart(ctx, taskID.String(), taskCreatedAt, end)
}

func (p *LokiProvider) QueryTurns(ctx context.Context, taskID uuid.UUID, start, end time.Time, limit int) (*QueryTurnsResp, error) {
	if p.client == nil {
		return nil, ErrProviderUnavailable
	}
	resp, err := p.client.QueryRounds(ctx, taskID.String(), start, end, limit)
	if err != nil {
		return nil, err
	}
	out := &QueryTurnsResp{
		Chunks:  make([]*RoundChunk, 0, len(resp.Chunks)),
		HasMore: resp.HasMore,
		NextTS:  resp.NextTS,
	}
	for _, chunk := range resp.Chunks {
		out.Chunks = append(out.Chunks, &RoundChunk{
			Data:      chunk.Data,
			Event:     chunk.Event,
			Kind:      chunk.Kind,
			Timestamp: chunk.Timestamp,
			Labels:    chunk.Labels,
		})
	}
	return out, nil
}

func (p *LokiProvider) QueryUserInputs(context.Context, []uuid.UUID, time.Time, int) ([]Entry, error) {
	return nil, ErrUnsupported
}

func (p *LokiProvider) CountEvents(context.Context, []uuid.UUID, []string) (int, error) {
	return 0, ErrUnsupported
}
