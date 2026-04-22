package tasklog

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Provider interface {
	Name() string
	QueryWindow(ctx context.Context, taskID uuid.UUID, start, end time.Time) ([]Entry, error)
	FindLatestTurnStart(ctx context.Context, taskID uuid.UUID, taskCreatedAt, end time.Time) (time.Time, error)
	QueryTurns(ctx context.Context, taskID uuid.UUID, start, end time.Time, limit int) (*QueryTurnsResp, error)
	QueryUserInputs(ctx context.Context, taskIDs []uuid.UUID, end time.Time, limit int) ([]Entry, error)
	CountEvents(ctx context.Context, taskIDs []uuid.UUID, events []string) (int, error)
}
