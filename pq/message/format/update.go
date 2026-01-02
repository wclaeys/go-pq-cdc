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
	MessageTime    time.Time
	NewTupleData   map[string]any
	OldTupleData   map[string]any
	TableNamespace string
	TableName      string
	OID            uint32 // Object IDentifier of the relation (table)
	XID            uint32 // Transaction ID of the transaction that caused this message
	OldTupleType   uint8  // 'K' message contains a key of the old tuple; 'O' message contains the old tuple itself (all columns)
	lsn            pq.LSN
}

func NewUpdate(data []byte, lsn pq.LSN, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Update, error) {
	msg := &Update{
		MessageTime: serverTime,
		lsn:         lsn,
	}
	if err := msg.decode(data, streamedTransaction, relation); err != nil {
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

func (m *Update) decode(data []byte, streamedTransaction bool, relation map[uint32]*Relation) error {
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

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	rel, ok := relation[m.OID]
	if !ok {
		return errors.Newf("relation %d not found", m.OID)
	}
	m.TableNamespace = rel.Namespace
	m.TableName = rel.Name

	m.OldTupleType = data[skipByte]

	var err error

	switch m.OldTupleType {
	case UpdateTupleTypeKey, UpdateTupleTypeOld:
		m.OldTupleData, skipByte, err = tuple.NewData(data, m.OldTupleType, skipByte, rel.Columns, nil)
		if err != nil {
			return errors.Wrap(err, "update message old tuple data")
		}
		fallthrough
	case UpdateTupleTypeNew:
		m.NewTupleData, _, err = tuple.NewData(data, UpdateTupleTypeNew, skipByte, rel.Columns, m.OldTupleData)
		if err != nil {
			return errors.Wrap(err, "update message new tuple data")
		}
	default:
		return errors.New("update message undefined tuple type")
	}

	return nil
}
