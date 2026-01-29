package core

import (
	"io"
	"sync"
)

// QueueDeliverySink allows transports to encode deliveries (meta + payload) without duplicating iteration.
type QueueDeliverySink interface {
	WriteMeta(idx, total int, d *QueueDelivery, defaultCursor string) error
	WritePayload(idx, total int, d *QueueDelivery) error
}

// WriteDeliveries walks deliveries and delegates encoding to the sink.
func WriteDeliveries(deliveries []*QueueDelivery, defaultCursor string, sink QueueDeliverySink) error {
	if sink == nil {
		return nil
	}
	total := len(deliveries)
	for i, d := range deliveries {
		if d == nil {
			continue
		}
		if err := sink.WriteMeta(i, total, d, defaultCursor); err != nil {
			return err
		}
		if d.Payload != nil {
			if err := sink.WritePayload(i, total, d); err != nil {
				return err
			}
		}
	}
	return nil
}

// PayloadCopy copies d.Payload to w; sink implementations can reuse it.
func PayloadCopy(w io.Writer, d *QueueDelivery) error {
	if d == nil || d.Payload == nil {
		return nil
	}
	buf := payloadCopyBufPool.Get().([]byte)
	_, err := io.CopyBuffer(w, d.Payload, buf)
	payloadCopyBufPool.Put(buf) //nolint:staticcheck // avoid extra allocation by pooling value slice
	return err
}

var payloadCopyBufPool = sync.Pool{
	New: func() any {
		// 32 KiB matches io.Copy default; tweakable if profiles suggest.
		return make([]byte, 32<<10)
	},
}
