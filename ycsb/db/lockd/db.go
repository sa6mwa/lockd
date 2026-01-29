package lockd

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/rs/xid"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

const (
	propEndpoints    = "lockd.endpoints"
	propNamespace    = "lockd.namespace"
	propBundle       = "lockd.bundle"
	propDisableMTLS  = "lockd.disable_mtls"
	propPublicRead   = "lockd.public_read"
	propLeaseTTL     = "lockd.lease_ttl"
	propAcquireBlock = "lockd.acquire_block"
	propOwnerPrefix  = "lockd.owner_prefix"
	propHTTPTimeout  = "lockd.http_timeout"
	propQueryEngine  = "lockd.query.engine"
	propQueryRefresh = "lockd.query.refresh"
	propQueryLimit   = "lockd.query.limit"
	propQueryReturn  = "lockd.query.return"
	propAttachEnable = "lockd.attach.enable"
	propAttachBytes  = "lockd.attach.bytes"
	propAttachRead   = "lockd.attach.read"
	propTxnExplicit  = "lockd.txn.explicit"
	propPhaseMetrics = "lockd.phase_metrics"
)

const attachmentDefaultName = "ycsb.bin"

func init() {
	ycsb.RegisterDBCreator("lockd", lockdCreator{})
}

type lockdCreator struct{}

type driverConfig struct {
	endpoints     []string
	namespace     string
	bundlePath    string
	disableMTLS   bool
	publicRead    bool
	leaseTTL      time.Duration
	blockSeconds  int64
	ownerPrefix   string
	httpTimeout   time.Duration
	queryEngine   string
	queryRefresh  string
	queryLimit    int
	queryReturn   lockdclient.QueryReturn
	attachEnable  bool
	attachBytes   int64
	attachRead    bool
	attachPayload []byte
	txnExplicit   bool
	phaseMetrics  bool
	phaseRecorder func(op string, start time.Time, dur time.Duration)
}

type lockdDB struct {
	client   *lockdclient.Client
	cfg      *driverConfig
	ownerSeq atomic.Uint64
}

type ownerKey struct{}

type recordDocument struct {
	Table     string            `json:"_table"`
	Key       string            `json:"_key"`
	Seq       int64             `json:"_seq"`
	UpdatedAt int64             `json:"_updated_at"`
	Data      map[string]string `json:"data"`
}

func (doc *recordDocument) normalize() {
	if doc.Data == nil {
		doc.Data = make(map[string]string)
	}
}

var errScanLimit = errors.New("lockd ycsb: scan limit satisfied")

func (lockdCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	cfg, err := parseConfig(p)
	if err != nil {
		return nil, err
	}
	httpClient, err := buildHTTPClient(cfg)
	if err != nil {
		return nil, err
	}
	cli, err := lockdclient.NewWithEndpoints(cfg.endpoints,
		lockdclient.WithHTTPClient(httpClient),
		lockdclient.WithDisableMTLS(cfg.disableMTLS),
		lockdclient.WithHTTPTimeout(cfg.httpTimeout),
		lockdclient.WithDefaultNamespace(cfg.namespace),
	)
	if err != nil {
		return nil, err
	}
	if cfg.phaseMetrics {
		cfg.phaseRecorder = measurement.Measure
	}
	return &lockdDB{client: cli, cfg: cfg}, nil
}

func parseConfig(p *properties.Properties) (*driverConfig, error) {
	rawEndpoints := strings.Split(p.GetString(propEndpoints, "https://127.0.0.1:9341"), ",")
	endpoints := make([]string, 0, len(rawEndpoints))
	for _, ep := range rawEndpoints {
		trimmed := strings.TrimSpace(ep)
		if trimmed != "" {
			endpoints = append(endpoints, trimmed)
		}
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("lockd ycsb: at least one endpoint required")
	}
	namespace := strings.TrimSpace(p.GetString(propNamespace, "default"))
	if namespace == "" {
		namespace = "default"
	}
	disableMTLS := p.GetBool(propDisableMTLS, false)
	bundlePath := strings.TrimSpace(p.GetString(propBundle, ""))
	if !disableMTLS && bundlePath == "" {
		return nil, fmt.Errorf("lockd ycsb: lockd.bundle is required unless lockd.disable_mtls=1")
	}
	leaseTTL := p.GetDuration(propLeaseTTL, 30*time.Second)
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	blockSecs := int64(p.GetInt(propAcquireBlock, 0))
	ownerPrefix := strings.TrimSpace(p.GetString(propOwnerPrefix, "ycsb-worker"))
	if ownerPrefix == "" {
		ownerPrefix = "ycsb-worker"
	}
	httpTimeout := p.GetDuration(propHTTPTimeout, 30*time.Second)
	if httpTimeout <= 0 {
		httpTimeout = 30 * time.Second
	}
	queryLimit := p.GetInt(propQueryLimit, 100)
	if queryLimit <= 0 {
		queryLimit = 100
	}
	queryReturn, err := parseQueryReturn(p.GetString(propQueryReturn, string(lockdclient.QueryReturnDocuments)))
	if err != nil {
		return nil, err
	}
	attachEnable := p.GetBool(propAttachEnable, false)
	attachBytes := int64(p.GetInt(propAttachBytes, 1024))
	if attachBytes <= 0 {
		attachBytes = 1024
	}
	attachRead := p.GetBool(propAttachRead, false)
	phaseMetrics := p.GetBool(propPhaseMetrics, false)
	cfg := &driverConfig{
		endpoints:    endpoints,
		namespace:    namespace,
		bundlePath:   bundlePath,
		disableMTLS:  disableMTLS,
		publicRead:   p.GetBool(propPublicRead, true),
		leaseTTL:     leaseTTL,
		blockSeconds: blockSecs,
		ownerPrefix:  ownerPrefix,
		httpTimeout:  httpTimeout,
		queryEngine:  strings.TrimSpace(strings.ToLower(p.GetString(propQueryEngine, "index"))),
		queryRefresh: strings.TrimSpace(strings.ToLower(p.GetString(propQueryRefresh, ""))),
		queryLimit:   queryLimit,
		queryReturn:  queryReturn,
		attachEnable: attachEnable,
		attachBytes:  attachBytes,
		attachRead:   attachRead,
		txnExplicit:  p.GetBool(propTxnExplicit, false),
		phaseMetrics: phaseMetrics,
	}
	if attachEnable {
		cfg.attachPayload = bytes.Repeat([]byte("y"), int(attachBytes))
	}
	return cfg, nil
}

func (db *lockdDB) recordPhase(op string, start time.Time) {
	if db == nil || db.cfg == nil || db.cfg.phaseRecorder == nil {
		return
	}
	db.cfg.phaseRecorder(op, start, time.Since(start))
}

func parseQueryReturn(raw string) (lockdclient.QueryReturn, error) {
	mode := strings.TrimSpace(strings.ToLower(raw))
	switch mode {
	case "", "documents", "document", "docs":
		return lockdclient.QueryReturnDocuments, nil
	case "keys", "key":
		return lockdclient.QueryReturnKeys, nil
	default:
		return "", fmt.Errorf("lockd ycsb: invalid lockd.query.return %q (expected keys or documents)", raw)
	}
}

func buildHTTPClient(cfg *driverConfig) (*http.Client, error) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("lockd ycsb: unexpected default transport type")
	}
	tr := transport.Clone()
	if cfg.disableMTLS {
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		material, pool, err := loadClientMaterial(cfg.bundlePath)
		if err != nil {
			return nil, err
		}
		tr.TLSClientConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			Certificates:       []tls.Certificate{*material},
			RootCAs:            pool,
			InsecureSkipVerify: true,
			VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
				return verifyPeerCertificate(rawCerts, pool)
			},
		}
	}
	return &http.Client{Transport: tr}, nil
}

func (db *lockdDB) Close() error {
	if db == nil || db.client == nil {
		return nil
	}
	return db.client.Close()
}

func (db *lockdDB) InitThread(ctx context.Context, threadID int, _ int) context.Context {
	suffix := db.ownerSeq.Add(1)
	owner := fmt.Sprintf("%s-%d-%d", db.cfg.ownerPrefix, threadID, suffix)
	return context.WithValue(ctx, ownerKey{}, owner)
}

func (db *lockdDB) CleanupThread(_ context.Context) {}

func (db *lockdDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	data, err := db.loadDocument(ctx, table, key)
	if err != nil {
		return nil, err
	}
	return filterFields(data.Data, fields), nil
}

func (db *lockdDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	if count <= 0 {
		return nil, nil
	}
	selector := api.Selector{And: []api.Selector{{Eq: &api.Term{Field: "/_table", Value: table}}}}
	if seq := sequenceFromKey(startKey); seq > 0 {
		seqVal := float64(seq)
		selector.And = append(selector.And, api.Selector{Range: &api.RangeTerm{Field: "/_seq", GTE: &seqVal}})
	}
	limit := count
	if db.cfg.queryLimit > 0 && count > db.cfg.queryLimit {
		limit = db.cfg.queryLimit
	}
	req := api.QueryRequest{Namespace: db.cfg.namespace, Selector: selector, Limit: limit}
	optFns := make([]lockdclient.QueryOption, 0, 4)
	switch db.cfg.queryEngine {
	case "index", "scan", "auto":
		optFns = append(optFns, lockdclient.WithQueryEngine(db.cfg.queryEngine))
	}
	if db.cfg.queryRefresh == "wait_for" {
		optFns = append(optFns, lockdclient.WithQueryRefreshWaitFor())
	}
	switch db.cfg.queryReturn {
	case lockdclient.QueryReturnDocuments:
		optFns = append(optFns, lockdclient.WithQueryReturnDocuments())
	case lockdclient.QueryReturnKeys:
		optFns = append(optFns, lockdclient.WithQueryReturnKeys())
	}
	optFns = append(optFns, lockdclient.WithQueryRequest(&req))
	resp, err := db.client.Query(ctx, optFns...)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("lockd ycsb: query returned no response")
	}
	defer resp.Close()

	results := make([]map[string][]byte, 0, min(count, limit))
	if resp.Mode() == lockdclient.QueryReturnDocuments {
		err := resp.ForEach(func(row lockdclient.QueryRow) error {
			if len(results) >= count {
				return errScanLimit
			}
			var doc recordDocument
			if err := row.DocumentInto(&doc); err != nil {
				return err
			}
			doc.normalize()
			if err := db.maybeReadAttachments(ctx, compositeKey(doc.Table, doc.Key), nil); err != nil {
				return err
			}
			results = append(results, filterFields(doc.Data, fields))
			return nil
		})
		if err != nil && !errors.Is(err, errScanLimit) {
			return nil, err
		}
		return results, nil
	}

	keys := resp.Keys()
	for _, composite := range keys {
		_, logical := splitKey(composite)
		if logical == "" {
			continue
		}
		doc, err := db.loadDocument(ctx, table, logical)
		if err != nil {
			return nil, err
		}
		doc.normalize()
		results = append(results, filterFields(doc.Data, fields))
		if len(results) >= count {
			break
		}
	}
	return results, nil
}

func (db *lockdDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.writeDocument(ctx, table, key, values)
}

func (db *lockdDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.writeDocument(ctx, table, key, values)
}

func (db *lockdDB) Delete(ctx context.Context, table string, key string) error {
	owner := db.ownerFromContext(ctx)
	lease, err := db.acquire(ctx, table, key, owner)
	if err != nil {
		return err
	}
	removeStart := time.Now()
	if _, err := lease.Remove(ctx); err != nil {
		db.recordPhase("LOCKD_REMOVE", removeStart)
		_ = lease.Release(ctx)
		return err
	}
	db.recordPhase("LOCKD_REMOVE", removeStart)
	releaseStart := time.Now()
	err = lease.Release(ctx)
	db.recordPhase("LOCKD_RELEASE", releaseStart)
	return err
}

func (db *lockdDB) acquire(ctx context.Context, table, key, owner string) (*lockdclient.LeaseSession, error) {
	ttlSeconds := int64(math.Ceil(db.cfg.leaseTTL.Seconds()))
	if ttlSeconds <= 0 {
		ttlSeconds = 1
	}
	composite := compositeKey(table, key)
	txnID := ""
	if db.cfg.txnExplicit {
		txnID = xid.New().String()
	}
	req := api.AcquireRequest{
		Namespace:  db.cfg.namespace,
		Key:        composite,
		Owner:      owner,
		TTLSeconds: ttlSeconds,
		BlockSecs:  db.cfg.blockSeconds,
		TxnID:      txnID,
	}
	start := time.Now()
	lease, err := db.client.Acquire(ctx, req)
	db.recordPhase("LOCKD_ACQUIRE", start)
	return lease, err
}

func (db *lockdDB) writeDocument(ctx context.Context, table, key string, values map[string][]byte) error {
	owner := db.ownerFromContext(ctx)
	lease, err := db.acquire(ctx, table, key, owner)
	if err != nil {
		return err
	}
	if values == nil {
		values = map[string][]byte{}
	}
	doc := recordDocument{
		Table:     table,
		Key:       key,
		Seq:       sequenceFromKey(key),
		UpdatedAt: time.Now().UnixNano(),
		Data:      map[string]string{},
	}
	for f, v := range values {
		doc.Data[f] = string(v)
	}
	saveOpts := make([]lockdclient.UpdateOption, 0, 1)
	if db.cfg.queryRefresh == "wait_for" {
		saveOpts = append(saveOpts, lockdclient.WithQueryVisible())
	}
	updateStart := time.Now()
	if err := lease.Save(ctx, doc, saveOpts...); err != nil {
		db.recordPhase("LOCKD_UPDATE", updateStart)
		_ = lease.Release(ctx)
		return err
	}
	db.recordPhase("LOCKD_UPDATE", updateStart)
	if err := db.maybeAttachPayload(ctx, lease); err != nil {
		_ = lease.Release(ctx)
		return err
	}
	releaseStart := time.Now()
	err = lease.Release(ctx)
	db.recordPhase("LOCKD_RELEASE", releaseStart)
	return err
}

func (db *lockdDB) loadDocument(ctx context.Context, table, key string) (*recordDocument, error) {
	composite := compositeKey(table, key)
	var (
		data []byte
		err  error
	)
	if db.cfg.publicRead {
		resp, errGet := db.client.Get(ctx, composite, lockdclient.WithGetNamespace(db.cfg.namespace))
		if errGet != nil {
			return nil, errGet
		}
		if resp != nil {
			defer resp.Close()
			data, err = resp.Bytes()
		}
		if err == nil && len(data) > 0 {
			if errAttach := db.maybeReadAttachments(ctx, composite, nil); errAttach != nil {
				return nil, errAttach
			}
		}
	} else {
		owner := db.ownerFromContext(ctx)
		lease, acquireErr := db.acquire(ctx, table, key, owner)
		if acquireErr != nil {
			return nil, acquireErr
		}
		resp, errGet := db.client.Get(ctx, composite,
			lockdclient.WithGetNamespace(db.cfg.namespace),
			lockdclient.WithGetLeaseID(lease.LeaseID),
			lockdclient.WithGetPublicDisabled(true),
		)
		if errGet == nil && resp != nil {
			defer resp.Close()
			data, err = resp.Bytes()
		}
		if err == nil && len(data) > 0 {
			if errAttach := db.maybeReadAttachments(ctx, composite, lease); errAttach != nil {
				_ = lease.Release(ctx)
				return nil, errAttach
			}
		}
		relErr := lease.Release(ctx)
		if errGet != nil {
			return nil, errGet
		}
		if err == nil && relErr != nil {
			err = relErr
		}
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("lockd ycsb: key %s not found", composite)
	}
	var doc recordDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	doc.normalize()
	return &doc, nil
}

func (db *lockdDB) ownerFromContext(ctx context.Context) string {
	if ctx == nil {
		return db.cfg.ownerPrefix
	}
	if v, ok := ctx.Value(ownerKey{}).(string); ok && v != "" {
		return v
	}
	return db.cfg.ownerPrefix
}

func compositeKey(table, key string) string {
	return table + "/" + key
}

func splitKey(composite string) (string, string) {
	parts := strings.SplitN(composite, "/", 2)
	if len(parts) != 2 {
		return "", composite
	}
	return parts[0], parts[1]
}

func filterFields(data map[string]string, fields []string) map[string][]byte {
	result := make(map[string][]byte)
	if data == nil {
		return result
	}
	if len(fields) == 0 {
		for k, v := range data {
			result[k] = []byte(v)
		}
		return result
	}
	for _, field := range fields {
		if val, ok := data[field]; ok {
			result[field] = []byte(val)
		}
	}
	return result
}

func sequenceFromKey(key string) int64 {
	end := len(key) - 1
	i := end
	for i >= 0 && key[i] >= '0' && key[i] <= '9' {
		i--
	}
	if i == end {
		return 0
	}
	seq, err := strconv.ParseInt(key[i+1:], 10, 64)
	if err != nil {
		return 0
	}
	return seq
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (db *lockdDB) maybeAttachPayload(ctx context.Context, lease *lockdclient.LeaseSession) error {
	if !db.cfg.attachEnable || lease == nil {
		return nil
	}
	body := bytes.NewReader(db.cfg.attachPayload)
	_, err := lease.Attach(ctx, lockdclient.AttachRequest{
		Name:        attachmentDefaultName,
		Body:        body,
		ContentType: "application/octet-stream",
		MaxBytes:    &db.cfg.attachBytes,
	})
	return err
}

func (db *lockdDB) maybeReadAttachments(ctx context.Context, key string, lease *lockdclient.LeaseSession) error {
	if !db.cfg.attachRead {
		return nil
	}
	if lease != nil {
		list, err := lease.ListAttachments(ctx)
		if err != nil || list == nil || len(list.Attachments) == 0 {
			return err
		}
		info := list.Attachments[0]
		att, err := lease.RetrieveAttachment(ctx, lockdclient.AttachmentSelector{ID: info.ID, Name: info.Name})
		if err != nil {
			return err
		}
		defer att.Close()
		_, err = io.Copy(io.Discard, att)
		return err
	}
	list, err := db.client.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{
		Namespace: db.cfg.namespace,
		Key:       key,
		Public:    true,
	})
	if err != nil || list == nil || len(list.Attachments) == 0 {
		return err
	}
	info := list.Attachments[0]
	att, err := db.client.GetAttachment(ctx, lockdclient.GetAttachmentRequest{
		Namespace: db.cfg.namespace,
		Key:       key,
		Public:    true,
		Selector:  lockdclient.AttachmentSelector{ID: info.ID, Name: info.Name},
	})
	if err != nil {
		return err
	}
	defer att.Close()
	_, err = io.Copy(io.Discard, att)
	return err
}

func loadClientMaterial(path string) (*tls.Certificate, *x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("lockd ycsb: read bundle: %w", err)
	}
	var (
		clientPEM []byte
		keyPEM    []byte
	)
	pool := x509.NewCertPool()
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		switch block.Type {
		case "CERTIFICATE":
			pemBytes := pem.EncodeToMemory(block)
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, nil, fmt.Errorf("lockd ycsb: parse certificate: %w", err)
			}
			if cert.IsCA {
				pool.AddCert(cert)
			} else {
				clientPEM = append(clientPEM, pemBytes...)
			}
		case "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY":
			if len(keyPEM) == 0 {
				keyPEM = pem.EncodeToMemory(block)
			}
		}
	}
	if len(clientPEM) == 0 {
		return nil, nil, fmt.Errorf("lockd ycsb: client certificate not found in %s", path)
	}
	if len(keyPEM) == 0 {
		return nil, nil, fmt.Errorf("lockd ycsb: client key not found in %s", path)
	}
	if pool == nil || len(pool.Subjects()) == 0 {
		return nil, nil, fmt.Errorf("lockd ycsb: CA certificate required in %s", path)
	}
	cert, err := tls.X509KeyPair(clientPEM, keyPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("lockd ycsb: build key pair: %w", err)
	}
	return &cert, pool, nil
}

func verifyPeerCertificate(rawCerts [][]byte, roots *x509.CertPool) error {
	if len(rawCerts) == 0 {
		return fmt.Errorf("lockd ycsb: missing server certificate")
	}
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("lockd ycsb: parse server certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	leaf := certs[0]
	opts := x509.VerifyOptions{
		Roots:         roots,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		Intermediates: x509.NewCertPool(),
		CurrentTime:   time.Now(),
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := leaf.Verify(opts); err != nil {
		return fmt.Errorf("lockd ycsb: verify server certificate: %w", err)
	}
	return nil
}
