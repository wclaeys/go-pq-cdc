package format

import (
	"encoding/binary"
	"errors"
	"time"
)

// LogicalMessage represents a pgoutput Message ('M') frame (pg_logical_emit_message).
// Wire layout (PG 14+):
//
//	'M' [Int32 xid if streamed] Int8 flags  Int64 lsn
//	    String prefix (NUL-terminated)      Int32 len  Bytes content
type LogicalMessage struct {
	MessageTime   time.Time
	Prefix        string
	Content       []byte
	LSN           uint64
	XID           uint32
	Transactional bool
}

// NewLogicalMessage parses an 'M' frame.
// Pass streamedTransaction=true if your stream is currently inside a streamed tx.
func NewLogicalMessage(data []byte, streamedTransaction bool, serverTime time.Time) (*LogicalMessage, error) {
	if len(data) < 1 || data[0] != 'M' {
		return nil, errors.New("logical message: invalid tag")
	}
	off := 1

	var xid uint32
	if streamedTransaction {
		if len(data) < off+4 {
			return nil, errors.New("logical message: short xid")
		}
		xid = binary.BigEndian.Uint32(data[off : off+4])
		off += 4
	}

	if len(data) < off+1+8 {
		return nil, errors.New("logical message: short header")
	}
	flags := data[off]
	off++
	lsn := binary.BigEndian.Uint64(data[off : off+8])
	off += 8

	// NUL-terminated prefix
	i := off
	for i < len(data) && data[i] != 0 {
		i++
	}
	if i >= len(data) {
		return nil, errors.New("logical message: unterminated prefix")
	}
	prefix := string(data[off:i])
	off = i + 1

	if len(data) < off+4 {
		return nil, errors.New("logical message: short length")
	}
	n := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4

	if n < 0 || len(data) < off+n {
		return nil, errors.New("logical message: short content")
	}

	return &LogicalMessage{
		MessageTime:   serverTime,
		Prefix:        prefix,
		Content:       data[off : off+n],
		Transactional: flags == 1,
		LSN:           lsn,
		XID:           xid,
	}, nil
}
