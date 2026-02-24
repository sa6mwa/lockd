package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"pkt.systems/lockd/internal/jsonutil"
)

func TestUpdateExpectedPayloadGuardsOptionalAndMatching(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	svc := newTestService(t)
	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          "update-expected-optional",
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	payload := "{\n  \"value\": 1\n}\n"
	sum := sha256.Sum256([]byte(payload))
	expectedSHA := hex.EncodeToString(sum[:])
	expectedBytes := int64(len(payload))

	res, err := svc.Update(ctx, UpdateCommand{
		Namespace:        "default",
		Key:              "update-expected-optional",
		LeaseID:          acq.LeaseID,
		FencingToken:     acq.FencingToken,
		TxnID:            acq.TxnID,
		IfVersion:        0,
		IfVersionSet:     true,
		ExpectedSHA256:   expectedSHA,
		ExpectedBytes:    expectedBytes,
		ExpectedBytesSet: true,
		Body:             strings.NewReader(payload),
		CompactWriter:    jsonutil.CompactWriter,
	})
	if err != nil {
		t.Fatalf("update with expected guards: %v", err)
	}
	if res == nil {
		t.Fatalf("expected update result")
	}
	if res.NewVersion != 1 {
		t.Fatalf("expected version 1, got %d", res.NewVersion)
	}

	res2, err := svc.Update(ctx, UpdateCommand{
		Namespace:     "default",
		Key:           "update-expected-optional",
		LeaseID:       acq.LeaseID,
		FencingToken:  acq.FencingToken,
		TxnID:         acq.TxnID,
		IfVersion:     1,
		IfVersionSet:  true,
		Body:          strings.NewReader(payload),
		CompactWriter: jsonutil.CompactWriter,
	})
	if err != nil {
		t.Fatalf("update without expected guards: %v", err)
	}
	if res2 == nil || res2.NewVersion != 1 {
		t.Fatalf("expected staged version 1 for same txn, got %+v", res2)
	}
}

func TestUpdateExpectedSHA256Mismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	svc := newTestService(t)
	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          "update-expected-sha-mismatch",
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	_, err = svc.Update(ctx, UpdateCommand{
		Namespace:      "default",
		Key:            "update-expected-sha-mismatch",
		LeaseID:        acq.LeaseID,
		FencingToken:   acq.FencingToken,
		TxnID:          acq.TxnID,
		ExpectedSHA256: strings.Repeat("0", 64),
		Body:           strings.NewReader(`{"value":1}`),
		CompactWriter: func(w io.Writer, r io.Reader, _ int64) error {
			_, copyErr := io.Copy(w, r)
			return copyErr
		},
	})
	if err == nil {
		t.Fatalf("expected sha mismatch error")
	}
	assertFailureCode(t, err, "expected_sha256_mismatch")
	var failure Failure
	if !strings.Contains(err.Error(), "expected_sha256_mismatch") {
		t.Fatalf("expected sha mismatch in error, got %v", err)
	}
	if !strings.Contains(err.Error(), "actual=") {
		t.Fatalf("expected actual checksum detail, got %v", err)
	}
	if !strings.Contains(err.Error(), "expected=") {
		t.Fatalf("expected expected checksum detail, got %v", err)
	}
	if !errors.As(err, &failure) {
		t.Fatalf("expected core failure, got %v", err)
	}
	if failure.HTTPStatus != http.StatusConflict {
		t.Fatalf("expected 409 status, got %d", failure.HTTPStatus)
	}
}

func TestUpdateExpectedBytesMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	svc := newTestService(t)
	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          "update-expected-bytes-mismatch",
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	_, err = svc.Update(ctx, UpdateCommand{
		Namespace:        "default",
		Key:              "update-expected-bytes-mismatch",
		LeaseID:          acq.LeaseID,
		FencingToken:     acq.FencingToken,
		TxnID:            acq.TxnID,
		ExpectedBytes:    int64(len(`{"value":1}`) + 1),
		ExpectedBytesSet: true,
		Body:             strings.NewReader(`{"value":1}`),
		CompactWriter: func(w io.Writer, r io.Reader, _ int64) error {
			_, copyErr := io.Copy(w, r)
			return copyErr
		},
	})
	if err == nil {
		t.Fatalf("expected bytes mismatch error")
	}
	assertFailureCode(t, err, "expected_bytes_mismatch")
}

func TestUpdateExpectedSHA256Invalid(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	svc := newTestService(t)
	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          "update-expected-sha-invalid",
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	_, err = svc.Update(ctx, UpdateCommand{
		Namespace:      "default",
		Key:            "update-expected-sha-invalid",
		LeaseID:        acq.LeaseID,
		FencingToken:   acq.FencingToken,
		TxnID:          acq.TxnID,
		ExpectedSHA256: "not-hex",
		Body:           strings.NewReader(`{"value":1}`),
		CompactWriter: func(w io.Writer, r io.Reader, _ int64) error {
			_, copyErr := io.Copy(w, r)
			return copyErr
		},
	})
	if err == nil {
		t.Fatalf("expected invalid expected_sha256 error")
	}
	assertFailureCode(t, err, "invalid_expected_sha256")
}
