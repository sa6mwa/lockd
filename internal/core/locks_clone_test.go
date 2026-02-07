package core

import (
	"testing"

	"pkt.systems/lockd/internal/storage"
)

func TestCloneMetaDeepCopiesMutableFields(t *testing.T) {
	t.Parallel()

	original := storage.Meta{
		Lease:                 &storage.Lease{ID: "lease-1", Owner: "owner-1", TxnID: "txn-1"},
		StateDescriptor:       []byte{1, 2, 3},
		StagedStateDescriptor: []byte{4, 5, 6},
		Attributes: map[string]string{
			"a": "1",
		},
		StagedAttributes: map[string]string{
			"s": "1",
		},
		Attachments: []storage.Attachment{
			{ID: "att-1", Descriptor: []byte{7, 8}},
		},
		StagedAttachments: []storage.StagedAttachment{
			{ID: "satt-1", StagedDescriptor: []byte{9, 10}},
		},
		StagedAttachmentDeletes: []string{"old"},
	}

	clone := cloneMeta(original)
	clone.Lease.Owner = "owner-2"
	clone.Attributes["a"] = "2"
	clone.StagedAttributes["s"] = "2"
	clone.StateDescriptor[0] = 99
	clone.StagedStateDescriptor[0] = 88
	clone.Attachments[0].Descriptor[0] = 77
	clone.StagedAttachments[0].StagedDescriptor[0] = 66
	clone.StagedAttachmentDeletes[0] = "new"

	if got := original.Lease.Owner; got != "owner-1" {
		t.Fatalf("lease aliasing: got %q", got)
	}
	if got := original.Attributes["a"]; got != "1" {
		t.Fatalf("attributes aliasing: got %q", got)
	}
	if got := original.StagedAttributes["s"]; got != "1" {
		t.Fatalf("staged attributes aliasing: got %q", got)
	}
	if got := original.StateDescriptor[0]; got != 1 {
		t.Fatalf("state descriptor aliasing: got %d", got)
	}
	if got := original.StagedStateDescriptor[0]; got != 4 {
		t.Fatalf("staged state descriptor aliasing: got %d", got)
	}
	if got := original.Attachments[0].Descriptor[0]; got != 7 {
		t.Fatalf("attachment descriptor aliasing: got %d", got)
	}
	if got := original.StagedAttachments[0].StagedDescriptor[0]; got != 9 {
		t.Fatalf("staged attachment descriptor aliasing: got %d", got)
	}
	if got := original.StagedAttachmentDeletes[0]; got != "old" {
		t.Fatalf("staged delete aliasing: got %q", got)
	}
}
