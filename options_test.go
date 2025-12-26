package dchan

import (
	"testing"
	"time"
)

func TestConfigSendCtx(t *testing.T) {
	config := &Config{
		SendTimeout: 100 * time.Millisecond,
	}

	ctx, cancel := config.sendCtx()
	if ctx == nil {
		t.Error("sendCtx returned nil context")
	}

	// Verify timeout is set
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Error("context should have a deadline")
	}

	now := time.Now()
	if deadline.Before(now) || deadline.After(now.Add(200*time.Millisecond)) {
		t.Errorf("deadline should be around 100ms from now, got %v", deadline.Sub(now))
	}

	cancel()
}

func TestConfigSendCtxNoTimeout(t *testing.T) {
	config := &Config{
		SendTimeout: 0,
	}

	ctx, cancel := config.sendCtx()
	if ctx == nil {
		t.Error("sendCtx returned nil context")
	}

	// Verify no timeout is set
	_, ok := ctx.Deadline()
	if ok {
		t.Error("context should not have a deadline when timeout is 0")
	}

	cancel()
}

func TestConfigClusterCtx(t *testing.T) {
	config := &Config{
		ClusterTimeout: 200 * time.Millisecond,
	}

	ctx, cancel := config.clusterCtx()
	if ctx == nil {
		t.Error("clusterCtx returned nil context")
	}

	// Verify timeout is set
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Error("context should have a deadline")
	}

	now := time.Now()
	if deadline.Before(now) || deadline.After(now.Add(300*time.Millisecond)) {
		t.Errorf("deadline should be around 200ms from now, got %v", deadline.Sub(now))
	}

	cancel()
}

func TestConfigClusterCtxNoTimeout(t *testing.T) {
	config := &Config{
		ClusterTimeout: 0,
	}

	ctx, cancel := config.clusterCtx()
	if ctx == nil {
		t.Error("clusterCtx returned nil context")
	}

	// Verify no timeout is set
	_, ok := ctx.Deadline()
	if ok {
		t.Error("context should not have a deadline when timeout is 0")
	}

	cancel()
}
