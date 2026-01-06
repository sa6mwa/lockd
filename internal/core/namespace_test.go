package core

import "testing"

func TestResolveNamespaceRejectsReserved(t *testing.T) {
	svc := &Service{defaultNamespace: "default"}

	if ns, err := svc.resolveNamespace("default"); err != nil || ns != "default" {
		t.Fatalf("expected default namespace, got ns=%q err=%v", ns, err)
	}

	if _, err := svc.resolveNamespace(".txns"); err == nil {
		t.Fatalf("expected error for reserved namespace .txns")
	}
	if _, err := svc.resolveNamespace(".hidden"); err == nil {
		t.Fatalf("expected error for reserved namespace .hidden")
	}
}
