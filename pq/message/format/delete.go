package format

import (
	"encoding/binary"
	"time"

	"github.com/go-playground/errors"
	"github.com/wclaeys/go-pq-cdc/pq"
	"github.com/wclaeys/go-pq-cdc/pq/message/tuple"
)

type Delete struct {
	MessageTime    time.Time
	OldTupleData   map[string]any
	TableNamespace string
	TableName      string
	OID            uint32 // Object IDentifier of the relation (table)
	XID            uint32 // Transaction ID of the transaction that caused this message
	OldTupleType   uint8  // 'K' message contains a key of the old tuple; 'O' message contains the old tuple itself (all columns)
	LSN            pq.LSN
}

func NewDelete(data []byte, lsn pq.LSN, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Delete, error) {
	msg := &Delete{
		MessageTime: serverTime,
		LSN:         lsn,
	}
	if err := msg.decode(data, streamedTransaction, relation); err != nil {
		return nil, err
	}

	return msg, nil
}

// Implements the WALMessage interface
func (m *Delete) GetLSN() pq.LSN {
	return m.LSN
}

// Implements the WALMessage interface
func (m *Delete) SetLSN(lsn pq.LSN) {
	m.LSN = lsn
}

func (m *Delete) decode(data []byte, streamedTransaction bool, relation map[uint32]*Relation) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 11 {
			return errors.Newf("streamed transaction delete message length must be at least 11 byte, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 7 {
		return errors.Newf("delete message length must be at least 7 byte, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	m.OldTupleType = data[skipByte]

	rel, ok := relation[m.OID]
	if !ok {
		return errors.Newf("relation %d not found", m.OID)
	}
	m.TableNamespace = rel.Namespace
	m.TableName = rel.Name

	var err error
	m.OldTupleData, _, err = tuple.NewData(data, m.OldTupleType, skipByte, rel.Columns, nil)
	if err != nil {
		return errors.Wrap(err, "delete message old tuple data")
	}

	return nil
}
