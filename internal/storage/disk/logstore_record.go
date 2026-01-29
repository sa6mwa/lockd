package disk

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/google/uuid"
)

const (
	logRecordMagic   = uint32(0x4c4f4744) // "LOGD"
	logRecordVersion = uint8(1)
	logHeaderSize    = 24
)

type logRecordType uint8

const (
	logRecordMetaPut logRecordType = iota + 1
	logRecordMetaDelete
	logRecordStatePut
	logRecordStateDelete
	logRecordObjectPut
	logRecordObjectDelete
	logRecordStateLink
)

type recordMeta struct {
	gen           uint64
	modifiedAt    int64
	etag          string
	contentType   string
	descriptor    []byte
	plaintextSize int64
	cipherSize    int64
}

type recordHeader struct {
	recType    logRecordType
	keyLen     uint32
	metaLen    uint32
	payloadLen uint32
	payloadCRC uint32
}

func encodeHeader(buf []byte, hdr recordHeader) {
	binary.LittleEndian.PutUint32(buf[0:4], logRecordMagic)
	buf[4] = logRecordVersion
	buf[5] = byte(hdr.recType)
	binary.LittleEndian.PutUint16(buf[6:8], 0)
	binary.LittleEndian.PutUint32(buf[8:12], hdr.keyLen)
	binary.LittleEndian.PutUint32(buf[12:16], hdr.metaLen)
	binary.LittleEndian.PutUint32(buf[16:20], hdr.payloadLen)
	binary.LittleEndian.PutUint32(buf[20:24], hdr.payloadCRC)
}

func decodeHeader(buf []byte) (recordHeader, error) {
	if len(buf) < logHeaderSize {
		return recordHeader{}, fmt.Errorf("disk: logstore header short read")
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != logRecordMagic {
		return recordHeader{}, fmt.Errorf("disk: logstore header magic mismatch")
	}
	if buf[4] != logRecordVersion {
		return recordHeader{}, fmt.Errorf("disk: logstore header version mismatch")
	}
	return recordHeader{
		recType:    logRecordType(buf[5]),
		keyLen:     binary.LittleEndian.Uint32(buf[8:12]),
		metaLen:    binary.LittleEndian.Uint32(buf[12:16]),
		payloadLen: binary.LittleEndian.Uint32(buf[16:20]),
		payloadCRC: binary.LittleEndian.Uint32(buf[20:24]),
	}, nil
}

func encodeMeta(recType logRecordType, meta recordMeta, etagBytes []byte) ([]byte, int, int, error) {
	return encodeMetaInto(nil, recType, meta, etagBytes)
}

func encodeMetaInto(buf []byte, recType logRecordType, meta recordMeta, etagBytes []byte) ([]byte, int, int, error) {
	size, _, err := metaSize(recType, meta)
	if err != nil {
		return nil, -1, 0, err
	}
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	switch recType {
	case logRecordMetaPut:
		const etagLenLocal = 16
		binary.LittleEndian.PutUint64(buf[0:8], meta.gen)
		binary.LittleEndian.PutUint64(buf[8:16], uint64(meta.modifiedAt))
		copy(buf[16:], ensureETagBytes(etagBytes, etagLenLocal))
		return buf, 16, etagLenLocal, nil
	case logRecordMetaDelete:
		binary.LittleEndian.PutUint64(buf[0:8], meta.gen)
		binary.LittleEndian.PutUint64(buf[8:16], uint64(meta.modifiedAt))
		return buf, -1, 0, nil
	case logRecordStatePut:
		const etagLenLocal = 32
		descLen := len(meta.descriptor)
		binary.LittleEndian.PutUint64(buf[0:8], meta.gen)
		binary.LittleEndian.PutUint64(buf[8:16], uint64(meta.modifiedAt))
		binary.LittleEndian.PutUint64(buf[16:24], uint64(meta.plaintextSize))
		binary.LittleEndian.PutUint64(buf[24:32], uint64(meta.cipherSize))
		copy(buf[32:32+etagLenLocal], ensureETagBytes(etagBytes, etagLenLocal))
		binary.LittleEndian.PutUint32(buf[32+etagLenLocal:32+etagLenLocal+4], uint32(descLen))
		copy(buf[32+etagLenLocal+4:], meta.descriptor)
		return buf, 32, etagLenLocal, nil
	case logRecordStateLink:
		const etagLenLocal = 32
		descLen := len(meta.descriptor)
		binary.LittleEndian.PutUint64(buf[0:8], meta.gen)
		binary.LittleEndian.PutUint64(buf[8:16], uint64(meta.modifiedAt))
		binary.LittleEndian.PutUint64(buf[16:24], uint64(meta.plaintextSize))
		binary.LittleEndian.PutUint64(buf[24:32], uint64(meta.cipherSize))
		copy(buf[32:32+etagLenLocal], ensureETagBytes(etagBytes, etagLenLocal))
		binary.LittleEndian.PutUint32(buf[32+etagLenLocal:32+etagLenLocal+4], uint32(descLen))
		copy(buf[32+etagLenLocal+4:], meta.descriptor)
		return buf, 32, etagLenLocal, nil
	case logRecordStateDelete:
		binary.LittleEndian.PutUint64(buf[0:8], meta.gen)
		binary.LittleEndian.PutUint64(buf[8:16], uint64(meta.modifiedAt))
		return buf, -1, 0, nil
	case logRecordObjectPut:
		const etagLenLocal = 32
		ctLen := len(meta.contentType)
		descLen := len(meta.descriptor)
		binary.LittleEndian.PutUint64(buf[0:8], meta.gen)
		binary.LittleEndian.PutUint64(buf[8:16], uint64(meta.modifiedAt))
		binary.LittleEndian.PutUint64(buf[16:24], uint64(meta.plaintextSize))
		copy(buf[24:24+etagLenLocal], ensureETagBytes(etagBytes, etagLenLocal))
		binary.LittleEndian.PutUint16(buf[24+etagLenLocal:24+etagLenLocal+2], uint16(ctLen))
		binary.LittleEndian.PutUint32(buf[24+etagLenLocal+2:24+etagLenLocal+2+4], uint32(descLen))
		offset := 24 + etagLenLocal + 2 + 4
		copy(buf[offset:offset+ctLen], meta.contentType)
		copy(buf[offset+ctLen:], meta.descriptor)
		return buf, 24, etagLenLocal, nil
	case logRecordObjectDelete:
		binary.LittleEndian.PutUint64(buf[0:8], meta.gen)
		binary.LittleEndian.PutUint64(buf[8:16], uint64(meta.modifiedAt))
		return buf, -1, 0, nil
	default:
		return nil, -1, 0, fmt.Errorf("disk: logstore unknown record type %d", recType)
	}
}

func metaSize(recType logRecordType, meta recordMeta) (int, int, error) {
	switch recType {
	case logRecordMetaPut:
		return 8 + 8 + 16, 16, nil
	case logRecordMetaDelete:
		return 8 + 8, 0, nil
	case logRecordStatePut, logRecordStateLink:
		descLen := len(meta.descriptor)
		return 8 + 8 + 8 + 8 + 32 + 4 + descLen, 32, nil
	case logRecordStateDelete:
		return 8 + 8, 0, nil
	case logRecordObjectPut:
		ctLen := len(meta.contentType)
		descLen := len(meta.descriptor)
		return 8 + 8 + 8 + 32 + 2 + 4 + ctLen + descLen, 32, nil
	case logRecordObjectDelete:
		return 8 + 8, 0, nil
	default:
		return 0, 0, fmt.Errorf("disk: logstore unknown record type %d", recType)
	}
}

func decodeMeta(recType logRecordType, data []byte) (recordMeta, error) {
	reader := bytes.NewReader(data)
	readUint64 := func() (uint64, error) {
		var buf [8]byte
		if _, err := io.ReadFull(reader, buf[:]); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(buf[:]), nil
	}
	switch recType {
	case logRecordMetaPut:
		gen, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		modified, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		etagBytes := make([]byte, 16)
		if _, err := io.ReadFull(reader, etagBytes); err != nil {
			return recordMeta{}, err
		}
		id, err := uuid.FromBytes(etagBytes)
		if err != nil {
			return recordMeta{}, err
		}
		return recordMeta{gen: gen, modifiedAt: int64(modified), etag: id.String()}, nil
	case logRecordMetaDelete:
		gen, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		modified, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		return recordMeta{gen: gen, modifiedAt: int64(modified)}, nil
	case logRecordStatePut:
		gen, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		modified, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		plain, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		cipher, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		etagBytes := make([]byte, 32)
		if _, err := io.ReadFull(reader, etagBytes); err != nil {
			return recordMeta{}, err
		}
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			return recordMeta{}, err
		}
		descLen := binary.LittleEndian.Uint32(lenBuf[:])
		descriptor := make([]byte, descLen)
		if _, err := io.ReadFull(reader, descriptor); err != nil {
			return recordMeta{}, err
		}
		return recordMeta{
			gen:           gen,
			modifiedAt:    int64(modified),
			etag:          hex.EncodeToString(etagBytes),
			plaintextSize: int64(plain),
			cipherSize:    int64(cipher),
			descriptor:    descriptor,
		}, nil
	case logRecordStateLink:
		gen, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		modified, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		plain, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		cipher, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		etagBytes := make([]byte, 32)
		if _, err := io.ReadFull(reader, etagBytes); err != nil {
			return recordMeta{}, err
		}
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			return recordMeta{}, err
		}
		descLen := binary.LittleEndian.Uint32(lenBuf[:])
		descriptor := make([]byte, descLen)
		if _, err := io.ReadFull(reader, descriptor); err != nil {
			return recordMeta{}, err
		}
		return recordMeta{
			gen:           gen,
			modifiedAt:    int64(modified),
			etag:          hex.EncodeToString(etagBytes),
			plaintextSize: int64(plain),
			cipherSize:    int64(cipher),
			descriptor:    descriptor,
		}, nil
	case logRecordStateDelete:
		gen, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		modified, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		return recordMeta{gen: gen, modifiedAt: int64(modified)}, nil
	case logRecordObjectPut:
		gen, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		modified, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		size, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		etagBytes := make([]byte, 32)
		if _, err := io.ReadFull(reader, etagBytes); err != nil {
			return recordMeta{}, err
		}
		var lenBuf [4]byte
		var lenBuf2 [2]byte
		if _, err := io.ReadFull(reader, lenBuf2[:]); err != nil {
			return recordMeta{}, err
		}
		ctLen := binary.LittleEndian.Uint16(lenBuf2[:])
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			return recordMeta{}, err
		}
		descLen := binary.LittleEndian.Uint32(lenBuf[:])
		contentType := make([]byte, ctLen)
		if _, err := io.ReadFull(reader, contentType); err != nil {
			return recordMeta{}, err
		}
		descriptor := make([]byte, descLen)
		if _, err := io.ReadFull(reader, descriptor); err != nil {
			return recordMeta{}, err
		}
		return recordMeta{
			gen:           gen,
			modifiedAt:    int64(modified),
			etag:          hex.EncodeToString(etagBytes),
			plaintextSize: int64(size),
			contentType:   string(contentType),
			descriptor:    descriptor,
		}, nil
	case logRecordObjectDelete:
		gen, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		modified, err := readUint64()
		if err != nil {
			return recordMeta{}, err
		}
		return recordMeta{gen: gen, modifiedAt: int64(modified)}, nil
	default:
		return recordMeta{}, fmt.Errorf("disk: logstore unknown record type %d", recType)
	}
}

var (
	zeroETag16 = make([]byte, 16)
	zeroETag32 = make([]byte, 32)
)

func ensureETagBytes(etag []byte, expected int) []byte {
	if len(etag) == expected {
		return etag
	}
	switch expected {
	case 16:
		return zeroETag16
	case 32:
		return zeroETag32
	default:
		return make([]byte, expected)
	}
}

func etagBytesForRecord(recType logRecordType, etag string) ([]byte, error) {
	switch recType {
	case logRecordMetaPut:
		id, err := uuid.Parse(etag)
		if err != nil {
			return nil, err
		}
		return id[:], nil
	case logRecordStatePut, logRecordStateLink, logRecordObjectPut:
		if etag == "" {
			return nil, nil
		}
		return hex.DecodeString(etag)
	default:
		return nil, nil
	}
}

type stateLinkPayload struct {
	segment string
	offset  int64
	length  int64
}

func encodeStateLinkPayload(link stateLinkPayload) ([]byte, error) {
	segmentBytes := []byte(link.segment)
	if len(segmentBytes) > 0xffff {
		return nil, fmt.Errorf("disk: logstore link segment too long")
	}
	buf := make([]byte, 2+8+8+len(segmentBytes))
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(segmentBytes)))
	binary.LittleEndian.PutUint64(buf[2:10], uint64(link.offset))
	binary.LittleEndian.PutUint64(buf[10:18], uint64(link.length))
	copy(buf[18:], segmentBytes)
	return buf, nil
}

func decodeStateLinkPayload(data []byte) (stateLinkPayload, error) {
	if len(data) < 18 {
		return stateLinkPayload{}, fmt.Errorf("disk: logstore link payload short")
	}
	segLen := int(binary.LittleEndian.Uint16(data[0:2]))
	if len(data) < 18+segLen {
		return stateLinkPayload{}, fmt.Errorf("disk: logstore link payload truncated")
	}
	offset := int64(binary.LittleEndian.Uint64(data[2:10]))
	length := int64(binary.LittleEndian.Uint64(data[10:18]))
	segment := string(data[18 : 18+segLen])
	return stateLinkPayload{segment: segment, offset: offset, length: length}, nil
}
