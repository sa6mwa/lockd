package mcp

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	lockdclient "pkt.systems/lockd/client"
)

func streamQueryRowsNDJSON(resp *lockdclient.QueryResponse) (io.ReadCloser, func()) {
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer resp.Close()
		if err := resp.ForEach(func(row lockdclient.QueryRow) error {
			if err := writeQueryRowNDJSON(pw, row); err != nil {
				return err
			}
			return nil
		}); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.Close()
	}()
	cleanup := func() {
		_ = pr.Close()
		_ = resp.Close()
		<-done
	}
	return pr, cleanup
}

func writeQueryRowNDJSON(w io.Writer, row lockdclient.QueryRow) error {
	if w == nil {
		return fmt.Errorf("writer required")
	}
	nsJSON, err := json.Marshal(row.Namespace)
	if err != nil {
		return err
	}
	keyJSON, err := json.Marshal(row.Key)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(w, `{"ns":`); err != nil {
		return err
	}
	if _, err := w.Write(nsJSON); err != nil {
		return err
	}
	if _, err := io.WriteString(w, `,"key":`); err != nil {
		return err
	}
	if _, err := w.Write(keyJSON); err != nil {
		return err
	}
	if row.Version > 0 {
		if _, err := io.WriteString(w, `,"ver":`+strconv.FormatInt(row.Version, 10)); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(w, `,"doc":`); err != nil {
		return err
	}
	docReader, err := row.DocumentReader()
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(w, docReader)
	closeErr := docReader.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	_, err = io.WriteString(w, "}\n")
	return err
}
