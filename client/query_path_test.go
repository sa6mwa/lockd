package client

import (
	"net/url"
	"testing"
)

func TestBuildQueryPath(t *testing.T) {
	t.Run("base path", func(t *testing.T) {
		got := buildQueryPath("", "", "", "")
		if got != "/v1/query" {
			t.Fatalf("buildQueryPath() = %q, want %q", got, "/v1/query")
		}
	})

	t.Run("with params", func(t *testing.T) {
		got := buildQueryPath("team/a b", "index", "wait_for", QueryReturnDocuments)
		u, err := url.Parse(got)
		if err != nil {
			t.Fatalf("parse path: %v", err)
		}
		if gotPath := u.Path; gotPath != "/v1/query" {
			t.Fatalf("path = %q, want %q", gotPath, "/v1/query")
		}
		q := u.Query()
		if q.Get("namespace") != "team/a b" {
			t.Fatalf("namespace = %q, want %q", q.Get("namespace"), "team/a b")
		}
		if q.Get("engine") != "index" {
			t.Fatalf("engine = %q, want %q", q.Get("engine"), "index")
		}
		if q.Get("refresh") != "wait_for" {
			t.Fatalf("refresh = %q, want %q", q.Get("refresh"), "wait_for")
		}
		if q.Get("return") != "documents" {
			t.Fatalf("return = %q, want %q", q.Get("return"), "documents")
		}
	})
}
