package format

import (
	"encoding/binary"
	"time"

	"github.com/go-playground/errors"
	"github.com/wclaeys/go-pq-cdc/pq"
	"github.com/wclaeys/go-pq-cdc/pq/message/tuple"
)

const (
	InsertTupleDataType = 'N'
)

type Insert struct {
	MessageTime    time.Time
	TupleData      map[string]any
	TableNamespace string
	TableName      string
	OID            uint32 // Object IDentifier of the relation (table)
	XID            uint32 // Transaction ID of the transaction that caused this message
	LSN            pq.LSN
}

func NewInsert(data []byte, lsn pq.LSN, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Insert, error) {
	msg := &Insert{
		MessageTime: serverTime,
		LSN:         lsn,
	}
	if err := msg.decode(data, streamedTransaction, relation); err != nil {
		return nil, err
	}

	return msg, nil
}

// Implements the WALMessage interface
func (m *Insert) GetLSN() pq.LSN {
	return m.LSN
}

// Implements the WALMessage interface
func (m *Insert) SetLSN(lsn pq.LSN) {
	m.LSN = lsn
}

func (m *Insert) decode(data []byte, streamedTransaction bool, relation map[uint32]*Relation) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 13 {
			return errors.Newf("streamed transaction insert message length must be at least 13 byte, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 9 {
		return errors.Newf("insert message length must be at least 9 byte, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	rel, ok := relation[m.OID]
	if !ok {
		return errors.Newf("relation %d not found", m.OID)
	}
	m.TableNamespace = rel.Namespace
	m.TableName = rel.Name

	var err error
	m.TupleData, _, err = tuple.NewData(data, InsertTupleDataType, skipByte, rel.Columns, nil)
	if err != nil {
		return errors.Wrap(err, "insert message")
	}

	return nil
}
