package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/chaitin/MonkeyCode/backend/config"
)

const TaskLogTable = "task_logs"

const CreateTaskLogTableSQL = `
CREATE TABLE IF NOT EXISTS task_logs (
	task_id UUID,
	ts DateTime64(9, 'UTC'),
	event LowCardinality(String),
	kind LowCardinality(String),
	turn_seq UInt32,
	data String,
	msg_seq String,
	source LowCardinality(String),
	log_version UInt16,
	ingest_id UUID
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (task_id, turn_seq, ts, ingest_id)
`

type Client struct {
	db    *sql.DB
	table string
}

func New(cfg config.ClickHouse, logger *slog.Logger) (*Client, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, nil
	}

	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if logger != nil {
		logger.With("component", "clickhouse").Info("clickhouse connection established")
	}
	return NewWithDB(db), nil
}

func NewWithDB(db *sql.DB) *Client {
	return &Client{db: db, table: TaskLogTable}
}

func (c *Client) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

func (c *Client) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}

func (c *Client) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

func buildDSN(cfg config.ClickHouse) (string, error) {
	addr := strings.TrimSpace(cfg.Addr)
	if addr == "" {
		return "", fmt.Errorf("clickhouse addr is empty")
	}
	if !strings.Contains(addr, "://") {
		addr = "clickhouse://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return "", err
	}
	if cfg.Username != "" {
		u.User = url.UserPassword(cfg.Username, cfg.Password)
	}
	if cfg.Database != "" {
		u.Path = "/" + strings.TrimPrefix(cfg.Database, "/")
	}
	return u.String(), nil
}
