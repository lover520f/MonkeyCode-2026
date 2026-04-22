package tasklog

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrProviderUnavailable = errors.New("tasklog provider unavailable")
	ErrUnsupported         = errors.New("tasklog operation unsupported")
)

type Entry struct {
	TaskID  uuid.UUID
	TS      time.Time
	Event   string
	Kind    string
	TurnSeq uint32
	Data    string
	MsgSeq  string
	Labels  map[string]string
}

type RoundChunk struct {
	Data      []byte
	Event     string
	Kind      string
	Timestamp int64
	Labels    map[string]string
}

type QueryTurnsResp struct {
	Chunks  []*RoundChunk
	HasMore bool
	NextTS  int64
}
