package tasklog

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/chaitin/MonkeyCode/backend/consts"
)

type Gateway struct {
	TaskStoreResolver func(context.Context, uuid.UUID) (consts.LogStore, error)
	Loki              Provider
	ClickHouse        Provider
}

func (g *Gateway) ProviderForTask(ctx context.Context, taskID uuid.UUID, source string) (Provider, error) {
	if source != "" {
		return g.providerByStore(consts.LogStore(source))
	}
	if g.TaskStoreResolver == nil {
		return nil, fmt.Errorf("resolve task log store: %w", ErrProviderUnavailable)
	}
	store, err := g.TaskStoreResolver(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("resolve task log store: %w", err)
	}
	return g.providerByStore(store)
}

func (g *Gateway) QueryWindow(ctx context.Context, taskID uuid.UUID, start, end time.Time, source string) ([]Entry, error) {
	p, err := g.ProviderForTask(ctx, taskID, source)
	if err != nil {
		return nil, err
	}
	return p.QueryWindow(ctx, taskID, start, end)
}

func (g *Gateway) FindLatestTurnStart(ctx context.Context, taskID uuid.UUID, taskCreatedAt, end time.Time, source string) (time.Time, error) {
	p, err := g.ProviderForTask(ctx, taskID, source)
	if err != nil {
		return time.Time{}, err
	}
	return p.FindLatestTurnStart(ctx, taskID, taskCreatedAt, end)
}

func (g *Gateway) QueryTurns(ctx context.Context, taskID uuid.UUID, start, end time.Time, limit int, source string) (*QueryTurnsResp, error) {
	p, err := g.ProviderForTask(ctx, taskID, source)
	if err != nil {
		return nil, err
	}
	return p.QueryTurns(ctx, taskID, start, end, limit)
}

func (g *Gateway) QueryUserInputs(ctx context.Context, taskID uuid.UUID, end time.Time, limit int, source string) ([]Entry, error) {
	p, err := g.ProviderForTask(ctx, taskID, source)
	if err != nil {
		return nil, err
	}
	return p.QueryUserInputs(ctx, []uuid.UUID{taskID}, end, limit)
}

func (g *Gateway) CountEvents(ctx context.Context, taskID uuid.UUID, events []string, source string) (int, error) {
	p, err := g.ProviderForTask(ctx, taskID, source)
	if err != nil {
		return 0, err
	}
	return p.CountEvents(ctx, []uuid.UUID{taskID}, events)
}

func (g *Gateway) providerByStore(store consts.LogStore) (Provider, error) {
	switch store {
	case consts.LogStoreLoki:
		if g.Loki == nil {
			return nil, fmt.Errorf("loki: %w", ErrProviderUnavailable)
		}
		return g.Loki, nil
	case consts.LogStoreClickHouse:
		if g.ClickHouse == nil {
			return nil, fmt.Errorf("clickhouse: %w", ErrProviderUnavailable)
		}
		return g.ClickHouse, nil
	default:
		return nil, fmt.Errorf("unknown log store %q: %w", store, ErrProviderUnavailable)
	}
}
