//go:build integration && azure

package azuretest

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

func TestCleanupBlobPrefixesStayScoped(t *testing.T) {
	prefixes := cleanupBlobPrefixes("it-run/azure/suite")
	if len(prefixes) != 1 {
		t.Fatalf("expected one scoped prefix, got %d", len(prefixes))
	}
	if prefixes[0] == nil {
		t.Fatalf("expected non-nil scoped prefix")
	}
	if got := *prefixes[0]; got != "it-run/azure/suite/" {
		t.Fatalf("unexpected scoped prefix %q", got)
	}
}

func TestCleanupBlobPrefixesAllowContainerWideWhenUnscoped(t *testing.T) {
	prefixes := cleanupBlobPrefixes("")
	if len(prefixes) != 1 {
		t.Fatalf("expected one prefix entry, got %d", len(prefixes))
	}
	if prefixes[0] != nil {
		t.Fatalf("expected nil prefix for unscoped cleanup, got %q", *prefixes[0])
	}
}

func TestAzureCleanupIgnoreDeleteError(t *testing.T) {
	if !azureCleanupIgnoreDeleteError(&azcore.ResponseError{StatusCode: http.StatusNotFound}) {
		t.Fatalf("expected 404 delete error to be ignored")
	}
	if azureCleanupIgnoreDeleteError(&azcore.ResponseError{StatusCode: http.StatusConflict}) {
		t.Fatalf("did not expect 409 delete error to be ignored")
	}
	if azureCleanupIgnoreDeleteError(errors.New("boom")) {
		t.Fatalf("did not expect generic error to be ignored")
	}
}

func TestRunScopedCleanupOnceCachesOnlySuccess(t *testing.T) {
	key := "azure-scope-success"
	delete(cleanupDone, key)
	calls := 0
	if err := runScopedCleanupOnce(key, func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("first cleanup: %v", err)
	}
	if err := runScopedCleanupOnce(key, func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected cleanup to run once after success, got %d", calls)
	}
}

func TestRunScopedCleanupOnceRetriesAfterFailure(t *testing.T) {
	key := "azure-scope-retry"
	delete(cleanupDone, key)
	calls := 0
	wantErr := errors.New("transient")
	if err := runScopedCleanupOnce(key, func() error {
		calls++
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("first cleanup error = %v, want %v", err, wantErr)
	}
	if err := runScopedCleanupOnce(key, func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected cleanup to retry after failure, got %d calls", calls)
	}
}
