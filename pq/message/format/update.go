package format

import (
	"encoding/binary"
	"time"

	"github.com/go-playground/errors"
	"github.com/wclaeys/go-pq-cdc/pq"
	"github.com/wclaeys/go-pq-cdc/pq/message/tuple"
)

const (
	UpdateTupleTypeKey = 'K'
	UpdateTupleTypeOld = 'O'
	UpdateTupleTypeNew = 'N'
)

type Update struct {
	MessageTime  time.Time
	Relation     *Relation
	NewTupleData []any
	OldTupleData []any
	lsn          pq.LSN
	XID          uint32
	OldTupleType uint8
}

func NewUpdate(data []byte, lsn pq.LSN, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time, decodeData bool) (*Update, error) {
	msg := &Update{
		MessageTime: serverTime,
		lsn:         lsn,
	}
	if err := msg.decode(data, streamedTransaction, relation, decodeData); err != nil {
		return nil, err
	}

	return msg, nil
}

// Implements the WALMessage interface
func (m *Update) GetLSN() pq.LSN {
	return m.lsn
}

// Implements the WALMessage interface
func (m *Update) SetLSN(lsn pq.LSN) {
	m.lsn = lsn
}

// Implements the DataWALMessage interface
func (m *Update) GetDecodedValue(columnIndex int) (any, error) {
	return tuple.GetDecodedValue(m.NewTupleData, m.Relation.Columns, columnIndex)
}

func (m *Update) GetDecodedOldValue(columnIndex int) (any, error) {
	return tuple.GetDecodedValue(m.OldTupleData, m.Relation.Columns, columnIndex)
}

func (m *Update) GetOldData() []any {
	return m.OldTupleData
}

// Implements the DataWALMessage interface
func (m *Update) GetData() []any {
	return m.NewTupleData
}

// Implements the DataWALMessage interface
func (m *Update) GetRelation() *Relation {
	return m.Relation
}

func (m *Update) decode(data []byte, streamedTransaction bool, relation map[uint32]*Relation, decodeData bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 11 {
			return errors.Newf("streamed transaction update message length must be at least 11 byte, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 7 {
		return errors.Newf("update message length must be at least 7 byte, but got %d", len(data))
	}

	OID := binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	rel, ok := relation[OID]
	if !ok {
		return errors.Newf("relation %d not found", OID)
	}
	m.Relation = rel

	m.OldTupleType = data[skipByte]

	var err error

	switch m.OldTupleType {
	case UpdateTupleTypeKey, UpdateTupleTypeOld:
		m.OldTupleData, skipByte, err = tuple.NewData(data, m.OldTupleType, skipByte, rel.Columns, decodeData, nil)
		if err != nil {
			return errors.Wrap(err, "update message old tuple data")
		}
		fallthrough
	case UpdateTupleTypeNew:
		m.NewTupleData, _, err = tuple.NewData(data, UpdateTupleTypeNew, skipByte, rel.Columns, decodeData, m.OldTupleData)
		if err != nil {
			return errors.Wrap(err, "update message new tuple data")
		}
	default:
		return errors.New("update message undefined tuple type")
	}

	return nil
}
