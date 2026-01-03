package format

import (
	"encoding/binary"
	"time"

	"github.com/go-playground/errors"
	"github.com/wclaeys/go-pq-cdc/pq"
	"github.com/wclaeys/go-pq-cdc/pq/message/tuple"
)

type Delete struct {
	MessageTime  time.Time
	Relation     *Relation
	OldTupleData []any
	LSN          pq.LSN
	XID          uint32
	OldTupleType uint8
}

func NewDelete(data []byte, lsn pq.LSN, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time, decodeData bool) (*Delete, error) {
	msg := &Delete{
		MessageTime: serverTime,
		LSN:         lsn,
	}
	if err := msg.decode(data, streamedTransaction, relation, decodeData); err != nil {
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

// Implements the DataWALMessage interface
func (m *Delete) GetDecodedValue(columnIndex int) (any, error) {
	return tuple.GetDecodedValue(m.OldTupleData, m.Relation.Columns, columnIndex)
}

// Implements the DataWALMessage interface
func (m *Delete) GetData() []any {
	return m.OldTupleData
}

// Implements the DataWALMessage interface
func (m *Delete) GetRelation() *Relation {
	return m.Relation
}

func (m *Delete) decode(data []byte, streamedTransaction bool, relation map[uint32]*Relation, decodeData bool) error {
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

	OID := binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	m.OldTupleType = data[skipByte]

	rel, ok := relation[OID]
	if !ok {
		return errors.Newf("relation %d not found", OID)
	}
	m.Relation = rel

	var err error
	m.OldTupleData, _, err = tuple.NewData(data, m.OldTupleType, skipByte, rel.Columns, decodeData, nil)
	if err != nil {
		return errors.Wrap(err, "delete message old tuple data")
	}

	return nil
}
