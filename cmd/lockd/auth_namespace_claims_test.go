package main

import (
	"testing"

	"pkt.systems/lockd/internal/nsauth"
)

func TestParseNamespacePermissionsCSVAndRepeat(t *testing.T) {
	perms, hasExplicit, err := parseNamespacePermissions(
		[]string{"default=rw,orders=w,stash", "orders=r"},
		false,
		false,
		false,
	)
	if err != nil {
		t.Fatalf("parse namespace permissions: %v", err)
	}
	if !hasExplicit {
		t.Fatal("expected explicit namespace claims")
	}
	if got := perms["default"]; got != nsauth.PermissionReadWrite {
		t.Fatalf("expected default rw, got %v", got)
	}
	if got := perms["orders"]; got != nsauth.PermissionWrite {
		t.Fatalf("expected orders w (strongest), got %v", got)
	}
	if got := perms["stash"]; got != nsauth.PermissionReadWrite {
		t.Fatalf("expected stash rw (implicit), got %v", got)
	}
}

func TestParseNamespacePermissionsAllAliases(t *testing.T) {
	perms, hasExplicit, err := parseNamespacePermissions(nil, true, true, false)
	if err != nil {
		t.Fatalf("parse namespace permissions: %v", err)
	}
	if !hasExplicit {
		t.Fatal("expected explicit namespace claims")
	}
	if got := perms[allNamespace]; got != nsauth.PermissionWrite {
		t.Fatalf("expected ALL w from read+write aliases, got %v", got)
	}

	perms, _, err = parseNamespacePermissions(nil, true, true, true)
	if err != nil {
		t.Fatalf("parse namespace permissions: %v", err)
	}
	if got := perms[allNamespace]; got != nsauth.PermissionReadWrite {
		t.Fatalf("expected ALL rw when --rw-all set, got %v", got)
	}
}

func TestParseNamespacePermissionsInvalid(t *testing.T) {
	if _, _, err := parseNamespacePermissions([]string{"default=x"}, false, false, false); err == nil {
		t.Fatal("expected invalid permission error")
	}
	if _, _, err := parseNamespacePermissions([]string{"=rw"}, false, false, false); err == nil {
		t.Fatal("expected missing namespace error")
	}
	if _, _, err := parseNamespacePermissions([]string{"default==rw"}, false, false, false); err == nil {
		t.Fatal("expected malformed claim error")
	}
}
