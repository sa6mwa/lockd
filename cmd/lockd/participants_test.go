package main

import "testing"

func TestParseParticipantsBackendHash(t *testing.T) {
	parts, err := parseParticipants([]string{"ns:key@hash1", "key2@hash2"}, "default")
	if err != nil {
		t.Fatalf("parse participants: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 participants, got %d", len(parts))
	}
	if parts[0].Namespace != "ns" || parts[0].Key != "key" || parts[0].BackendHash != "hash1" {
		t.Fatalf("unexpected participant[0]: %+v", parts[0])
	}
	if parts[1].Namespace != "default" || parts[1].Key != "key2" || parts[1].BackendHash != "hash2" {
		t.Fatalf("unexpected participant[1]: %+v", parts[1])
	}
}

func TestParseParticipantsRejectsEmptyBackendHash(t *testing.T) {
	if _, err := parseParticipants([]string{"ns:key@"}, "default"); err == nil {
		t.Fatalf("expected error for empty backend hash")
	}
}
