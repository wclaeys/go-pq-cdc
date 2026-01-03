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
	MessageTime time.Time
	Relation    *Relation
	TupleData   []any
	LSN         pq.LSN
	XID         uint32
}

func NewInsert(data []byte, lsn pq.LSN, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time, decodeData bool) (*Insert, error) {
	msg := &Insert{
		MessageTime: serverTime,
		LSN:         lsn,
	}
	if err := msg.decode(data, streamedTransaction, relation, decodeData); err != nil {
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

// Implements the DataWALMessage interface
func (m *Insert) GetDecodedValue(columnIndex int) (any, error) {
	return tuple.GetDecodedValue(m.TupleData, m.Relation.Columns, columnIndex)
}

// Implements the DataWALMessage interface
func (m *Insert) GetData() []any {
	return m.TupleData
}

// Implements the DataWALMessage interface
func (m *Insert) GetRelation() *Relation {
	return m.Relation
}

func (m *Insert) decode(data []byte, streamedTransaction bool, relation map[uint32]*Relation, decodeData bool) error {
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

	OID := binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	rel, ok := relation[OID]
	if !ok {
		return errors.Newf("relation %d not found", OID)
	}
	m.Relation = rel

	var err error
	m.TupleData, _, err = tuple.NewData(data, InsertTupleDataType, skipByte, rel.Columns, decodeData, nil)
	if err != nil {
		return errors.Wrap(err, "insert message")
	}

	return nil
}
