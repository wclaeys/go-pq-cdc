package format

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/wclaeys/go-pq-cdc/pq"
)

// Truncate represents a pgoutput TRUNCATE message.
// https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
type Truncate struct {
	MessageTime     time.Time
	Relations       []TruncateRelation
	XID             uint32 // Transaction ID of the transaction that caused this message
	Cascade         bool
	RestartIdentity bool
	lsn             pq.LSN
}

type TruncateRelation struct {
	TableNamespace string
	TableName      string
	OID            uint32
}

// Implements the WALMessage interface
func (m *Truncate) GetLSN() pq.LSN {
	return m.lsn
}

// Implements the WALMessage interface
func (m *Truncate) SetLSN(lsn pq.LSN) {
	m.lsn = lsn
}

// NewTruncate parses the TRUNCATE message body (after the leading 'T' byte).
// If streamedTransaction is true, the XID is present at the front of the message.
func NewTruncate(data []byte, lsn pq.LSN, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Truncate, error) {
	t := &Truncate{
		MessageTime: serverTime,
		lsn:         lsn,
	}

	skipByte := 1
	// XID (only when streaming a big tx; protocol v2+)
	if streamedTransaction {
		if len(data) < skipByte+4 {
			return nil, fmt.Errorf("truncate: missing xid")
		}
		t.XID = binary.BigEndian.Uint32(data[skipByte : skipByte+4])
		skipByte += 4
	}

	// Number of relations (int32)
	if len(data) < skipByte+4 {
		return nil, fmt.Errorf("truncate: missing relations count")
	}
	n := int(binary.BigEndian.Uint32(data[skipByte : skipByte+4]))
	skipByte += 4

	// Options (int8): 1 = CASCADE, 2 = RESTART IDENTITY
	if len(data) < skipByte+1 {
		return nil, fmt.Errorf("truncate: missing options")
	}
	opts := data[skipByte]
	skipByte++

	t.Cascade = (opts & 0x01) != 0
	t.RestartIdentity = (opts & 0x02) != 0

	// Read OIDs and map to known relations
	t.Relations = make([]TruncateRelation, 0, n)
	for i := range n {
		if len(data) < skipByte+4 {
			return nil, fmt.Errorf("truncate: missing relation oid %d", i)
		}
		oid := binary.BigEndian.Uint32(data[skipByte : skipByte+4])
		skipByte += 4

		rel, ok := relation[oid]
		if !ok {
			return nil, fmt.Errorf("truncate: unknown relation oid=%d", oid)
		}

		t.Relations = append(t.Relations, TruncateRelation{
			TableNamespace: rel.Namespace,
			TableName:      rel.Name,
			OID:            oid,
		})
	}

	return t, nil
}
