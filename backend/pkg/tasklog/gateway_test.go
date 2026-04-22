package tasklog_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/chaitin/MonkeyCode/backend/consts"
	"github.com/chaitin/MonkeyCode/backend/pkg/tasklog"
)

type providerStub struct {
	name string
}

func (p *providerStub) Name() string {
	return p.name
}

func (p *providerStub) QueryLatestTurn(context.Context, uuid.UUID, time.Time, time.Time) (*tasklog.QueryLatestTurnResp, error) {
	return nil, nil
}

func (p *providerStub) QueryTurns(context.Context, uuid.UUID, time.Time, string, int) (*tasklog.QueryTurnsResp, error) {
	return nil, nil
}

func TestGatewayRoutesByTaskLogStore(t *testing.T) {
	gw := &tasklog.Gateway{
		TaskStoreResolver: func(context.Context, uuid.UUID) (consts.LogStore, error) {
			return consts.LogStoreClickHouse, nil
		},
		Loki:       &providerStub{name: "loki"},
		ClickHouse: &providerStub{name: "clickhouse"},
	}

	got, err := gw.ProviderForTask(context.Background(), uuid.MustParse("11111111-1111-1111-1111-111111111111"), "")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name() != "clickhouse" {
		t.Fatalf("provider = %s, want clickhouse", got.Name())
	}
}

func TestGatewayOverrideSource(t *testing.T) {
	gw := &tasklog.Gateway{
		TaskStoreResolver: func(context.Context, uuid.UUID) (consts.LogStore, error) {
			return consts.LogStoreClickHouse, nil
		},
		Loki:       &providerStub{name: "loki"},
		ClickHouse: &providerStub{name: "clickhouse"},
	}

	got, err := gw.ProviderForTask(context.Background(), uuid.Nil, "loki")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name() != "loki" {
		t.Fatalf("provider = %s, want loki", got.Name())
	}
}
