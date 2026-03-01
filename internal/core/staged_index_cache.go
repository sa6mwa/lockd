package core

import (
	"strings"

	indexer "pkt.systems/lockd/internal/search/index"
)

func stagedIndexCacheKey(namespace, key, txnID, stagedStateETag string) string {
	return namespace + "\x00" + key + "\x00" + txnID + "\x00" + stagedStateETag
}

func stagedIndexHeadKey(namespace, key, txnID string) string {
	return namespace + "\x00" + key + "\x00" + txnID
}

func (s *Service) cacheStagedIndexDoc(namespace, key, txnID, stagedStateETag string, plaintextBytes int64, doc indexer.Document) {
	if s == nil || s.indexManager == nil {
		return
	}
	if plaintextBytes <= 0 || plaintextBytes > maxStagedIndexDocBytes {
		return
	}
	namespace = strings.TrimSpace(namespace)
	key = strings.TrimSpace(key)
	txnID = strings.TrimSpace(txnID)
	stagedStateETag = strings.TrimSpace(stagedStateETag)
	if namespace == "" || key == "" || txnID == "" || stagedStateETag == "" {
		return
	}
	docKey := stagedIndexCacheKey(namespace, key, txnID, stagedStateETag)
	headKey := stagedIndexHeadKey(namespace, key, txnID)

	s.stagedIndexDocsMu.Lock()
	defer s.stagedIndexDocsMu.Unlock()

	if priorRaw, ok := s.stagedIndexDocHeads.Load(headKey); ok {
		if priorETag, ok := priorRaw.(string); ok && priorETag != "" && priorETag != stagedStateETag {
			s.stagedIndexDocs.Delete(stagedIndexCacheKey(namespace, key, txnID, priorETag))
		}
	}
	s.stagedIndexDocs.Store(docKey, doc)
	s.stagedIndexDocHeads.Store(headKey, stagedStateETag)
}

func (s *Service) stagedIndexDoc(namespace, key, txnID, stagedStateETag string) (indexer.Document, bool) {
	if s == nil {
		return indexer.Document{}, false
	}
	namespace = strings.TrimSpace(namespace)
	key = strings.TrimSpace(key)
	txnID = strings.TrimSpace(txnID)
	stagedStateETag = strings.TrimSpace(stagedStateETag)
	if namespace == "" || key == "" || txnID == "" || stagedStateETag == "" {
		return indexer.Document{}, false
	}
	value, ok := s.stagedIndexDocs.Load(stagedIndexCacheKey(namespace, key, txnID, stagedStateETag))
	if !ok {
		return indexer.Document{}, false
	}
	doc, ok := value.(indexer.Document)
	if !ok {
		return indexer.Document{}, false
	}
	return doc, true
}

func (s *Service) deleteStagedIndexDoc(namespace, key, txnID, stagedStateETag string) {
	if s == nil {
		return
	}
	namespace = strings.TrimSpace(namespace)
	key = strings.TrimSpace(key)
	txnID = strings.TrimSpace(txnID)
	stagedStateETag = strings.TrimSpace(stagedStateETag)
	if namespace == "" || key == "" || txnID == "" || stagedStateETag == "" {
		return
	}
	docKey := stagedIndexCacheKey(namespace, key, txnID, stagedStateETag)
	headKey := stagedIndexHeadKey(namespace, key, txnID)

	s.stagedIndexDocsMu.Lock()
	defer s.stagedIndexDocsMu.Unlock()

	s.stagedIndexDocs.Delete(docKey)
	if priorRaw, ok := s.stagedIndexDocHeads.Load(headKey); ok {
		if priorETag, ok := priorRaw.(string); ok && priorETag == stagedStateETag {
			s.stagedIndexDocHeads.Delete(headKey)
		}
	}
}
