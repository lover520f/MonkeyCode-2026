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

func (g *Gateway) QueryLatestTurn(ctx context.Context, taskID uuid.UUID, taskCreatedAt, end time.Time, source string) (*QueryLatestTurnResp, error) {
	p, err := g.ProviderForTask(ctx, taskID, source)
	if err != nil {
		return nil, err
	}
	return p.QueryLatestTurn(ctx, taskID, taskCreatedAt, end)
}

func (g *Gateway) QueryTurns(ctx context.Context, taskID uuid.UUID, taskCreatedAt time.Time, cursor string, limit int, source string) (*QueryTurnsResp, error) {
	p, err := g.ProviderForTask(ctx, taskID, source)
	if err != nil {
		return nil, err
	}
	return p.QueryTurns(ctx, taskID, taskCreatedAt, cursor, limit)
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
