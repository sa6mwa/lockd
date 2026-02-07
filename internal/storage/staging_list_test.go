package storage

import (
	"context"
	"sort"
	"testing"
)

type fakeStagedObjectLister struct {
	keys []string
}

func (f *fakeStagedObjectLister) ListObjects(_ context.Context, _ string, opts ListOptions) (*ListResult, error) {
	keys := append([]string(nil), f.keys...)
	sort.Strings(keys)
	start := 0
	if opts.StartAfter != "" {
		for start < len(keys) && keys[start] <= opts.StartAfter {
			start++
		}
	}
	limit := opts.Limit
	if limit <= 0 || start+limit > len(keys) {
		limit = len(keys) - start
	}
	end := start + limit
	objects := make([]ObjectInfo, 0, end-start)
	for _, key := range keys[start:end] {
		objects = append(objects, ObjectInfo{Key: key})
	}
	out := &ListResult{Objects: objects}
	if end < len(keys) {
		out.Truncated = true
		out.NextStartAfter = keys[end-1]
	}
	return out, nil
}

func TestStagedStateTxnID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		want  string
		match bool
	}{
		{name: "root", key: ".staging/txn-1", want: "txn-1", match: true},
		{name: "per_key", key: "orders/1/.staging/txn-2", want: "txn-2", match: true},
		{name: "attachment", key: "state/orders/1/.staging/txn-2/attachments/a1", match: false},
		{name: "not_staged", key: "orders/1/state", match: false},
		{name: "missing_txn", key: "orders/1/.staging/", match: false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := stagedStateTxnID(tc.key)
			if ok != tc.match {
				t.Fatalf("match=%v want %v for %q", ok, tc.match, tc.key)
			}
			if got != tc.want {
				t.Fatalf("txn id = %q want %q", got, tc.want)
			}
		})
	}
}

func TestListStagedStateFromObjectsFiltersAndExcludesNested(t *testing.T) {
	t.Parallel()

	lister := &fakeStagedObjectLister{
		keys: []string{
			".staging/txn-root",
			"alpha/.staging/txn-a",
			"alpha/.staging/txn-a/attachments/a1",
			"beta/.staging/skip",
			"index/manifest.json",
			"state/alpha/.staging/txn-b/attachments/a2",
		},
	}
	list, err := ListStagedStateFromObjects(context.Background(), lister, "default", ListStagedOptions{TxnPrefix: "txn-"})
	if err != nil {
		t.Fatalf("list staged state: %v", err)
	}
	if len(list.Objects) != 2 {
		t.Fatalf("expected 2 staged state objects, got %d", len(list.Objects))
	}
	if got := list.Objects[0].Key; got != ".staging/txn-root" {
		t.Fatalf("unexpected object[0]: %q", got)
	}
	if got := list.Objects[1].Key; got != "alpha/.staging/txn-a" {
		t.Fatalf("unexpected object[1]: %q", got)
	}
}

func TestListStagedStateFromObjectsPagination(t *testing.T) {
	t.Parallel()

	lister := &fakeStagedObjectLister{
		keys: []string{
			"alpha/.staging/txn-1",
			"beta/.staging/txn-2",
			"beta/.staging/txn-2/attachments/a1",
			"gamma/.staging/txn-3",
			"index/manifest.json",
		},
	}
	first, err := ListStagedStateFromObjects(context.Background(), lister, "default", ListStagedOptions{Limit: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first.Objects) != 2 {
		t.Fatalf("expected 2 objects on first page, got %d", len(first.Objects))
	}
	if !first.Truncated {
		t.Fatalf("expected first page to be truncated")
	}
	second, err := ListStagedStateFromObjects(context.Background(), lister, "default", ListStagedOptions{
		StartAfter: first.NextStartAfter,
		Limit:      2,
	})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(second.Objects) != 1 {
		t.Fatalf("expected 1 object on second page, got %d", len(second.Objects))
	}
	if second.Truncated {
		t.Fatalf("expected second page to be complete")
	}
	if got := second.Objects[0].Key; got != "gamma/.staging/txn-3" {
		t.Fatalf("unexpected second page object: %q", got)
	}
}

func TestIsStagingObjectKey(t *testing.T) {
	t.Parallel()

	if !IsStagingObjectKey(".staging/root-txn") {
		t.Fatalf("expected root staged key to match")
	}
	if !IsStagingObjectKey("alpha/.staging/txn-1/attachments/a1") {
		t.Fatalf("expected nested staged key to match")
	}
	if IsStagingObjectKey("alpha/staging/txn-1") {
		t.Fatalf("unexpected match for non-staging key")
	}
}
