package format

import (
	"encoding/binary"
	"fmt"
	"time"
)

// Truncate represents a pgoutput TRUNCATE message.
// https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
type Truncate struct {
	MessageTime     time.Time
	XID             uint32 // present only for streamed transactions (protocol v2+)
	Cascade         bool   // option bit 1
	RestartIdentity bool   // option bit 2
	Relations       []TruncateRelation
}

type TruncateRelation struct {
	TableNamespace string
	TableName      string
	OID            uint32
}

// NewTruncate parses the TRUNCATE message body (after the leading 'T' byte).
// If streamedTransaction is true, the XID is present at the front of the message.
func NewTruncate(
	data []byte,
	streamedTransaction bool,
	relation map[uint32]*Relation,
	serverTime time.Time,
) (*Truncate, error) {

	skipByte := 1
	t := &Truncate{MessageTime: serverTime}

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
	n := int(int32(binary.BigEndian.Uint32(data[skipByte : skipByte+4])))
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
