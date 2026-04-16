package v1

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/chaitin/MonkeyCode/backend/db"
	"github.com/chaitin/MonkeyCode/backend/domain"
	"github.com/chaitin/MonkeyCode/backend/pkg/taskflow"
)

func TestAgentAuthRecycledVMTriggersDeleteOnce(t *testing.T) {
	vmClient := &vmDeleterStub{}
	handler := &InternalHostHandler{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		repo: &internalHostRepoStub{
			vm: &db.VirtualMachine{
				ID:            "agent_1",
				HostID:        "host_1",
				EnvironmentID: "env_1",
				UserID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				IsRecycled:    true,
			},
		},
		vmDeleter:      vmClient,
		limiter:        &setNXLimiterStub{result: true},
		skipSoftDelete: func(ctx context.Context) context.Context { return ctx },
		runAsync:       func(fn func()) { fn() },
	}

	_, err := handler.agentAuth(context.Background(), "agent_1", "machine-1")
	if err == nil {
		t.Fatal("expected recycled vm auth to fail")
	}
	if len(vmClient.reqs) != 1 {
		t.Fatalf("delete calls = %d, want 1", len(vmClient.reqs))
	}
	if vmClient.reqs[0].ID != "env_1" {
		t.Fatalf("delete env id = %q, want env_1", vmClient.reqs[0].ID)
	}
}

func TestAgentAuthRecycledVMLimitedSkipsDelete(t *testing.T) {
	vmClient := &vmDeleterStub{}
	handler := &InternalHostHandler{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		repo: &internalHostRepoStub{
			vm: &db.VirtualMachine{
				ID:            "agent_2",
				HostID:        "host_2",
				EnvironmentID: "env_2",
				UserID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				IsRecycled:    true,
			},
		},
		vmDeleter:      vmClient,
		limiter:        &setNXLimiterStub{result: false},
		skipSoftDelete: func(ctx context.Context) context.Context { return ctx },
		runAsync:       func(fn func()) { fn() },
	}

	_, err := handler.agentAuth(context.Background(), "agent_2", "machine-2")
	if err == nil {
		t.Fatal("expected recycled vm auth to fail")
	}
	if len(vmClient.reqs) != 0 {
		t.Fatalf("delete calls = %d, want 0", len(vmClient.reqs))
	}
}

type internalHostRepoStub struct {
	vm *db.VirtualMachine
}

func (s *internalHostRepoStub) UpsertHost(context.Context, *taskflow.Host) error {
	return nil
}

func (s *internalHostRepoStub) UpsertVirtualMachine(context.Context, *taskflow.VirtualMachine) error {
	return nil
}

func (s *internalHostRepoStub) GetVirtualMachine(context.Context, string) (*db.VirtualMachine, error) {
	if s.vm == nil {
		return nil, errors.New("vm not found")
	}
	return s.vm, nil
}

func (s *internalHostRepoStub) UpdateVirtualMachine(context.Context, string, func(*db.VirtualMachineUpdateOne) error) error {
	return nil
}

func (s *internalHostRepoStub) GetByID(context.Context, string) (*db.Host, error) {
	return nil, errors.New("host not found")
}

func (s *internalHostRepoStub) GetVirtualMachineByEnvID(context.Context, string) (*db.VirtualMachine, error) {
	return nil, errors.New("vm not found")
}

func (s *internalHostRepoStub) GetGitCredentialByTask(context.Context, string) (*domain.GitCredentialInfo, error) {
	return nil, errors.New("task not found")
}

type setNXLimiterStub struct {
	result bool
	err    error
	keys   []string
	ttl    time.Duration
}

func (s *setNXLimiterStub) SetNX(_ context.Context, key string, _ interface{}, ttl time.Duration) *redis.BoolCmd {
	s.keys = append(s.keys, key)
	s.ttl = ttl
	return redis.NewBoolResult(s.result, s.err)
}

type vmDeleterStub struct {
	reqs []*taskflow.DeleteVirtualMachineReq
	err  error
}

func (s *vmDeleterStub) Delete(_ context.Context, req *taskflow.DeleteVirtualMachineReq) error {
	cp := *req
	s.reqs = append(s.reqs, &cp)
	return s.err
}
