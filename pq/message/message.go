package message

import (
	"time"

	"github.com/go-playground/errors"
	"github.com/wclaeys/go-pq-cdc/pq"
	"github.com/wclaeys/go-pq-cdc/pq/message/format"
)

const (
	StreamAbortByte  Type = 'A'
	BeginByte        Type = 'B'
	CommitByte       Type = 'C'
	DeleteByte       Type = 'D'
	StreamStopByte   Type = 'E'
	InsertByte       Type = 'I'
	LogicalByte      Type = 'M'
	OriginByte       Type = 'O'
	RelationByte     Type = 'R'
	StreamStartByte  Type = 'S'
	TruncateByte     Type = 'T'
	UpdateByte       Type = 'U'
	TypeByte         Type = 'Y'
	StreamCommitByte Type = 'c'
)

const (
	XLogDataByteID                = 'w'
	PrimaryKeepaliveMessageByteID = 'k'
)

var ErrorByteNotSupported = errors.New("message byte not supported")

type Type uint8

var streamedTransaction bool

func New(data []byte, walStart pq.LSN, serverTime time.Time, relation map[uint32]*format.Relation) (format.WALMessage, error) {
	switch Type(data[0]) {
	case InsertByte:
		return format.NewInsert(data, walStart, streamedTransaction, relation, serverTime)
	case UpdateByte:
		return format.NewUpdate(data, walStart, streamedTransaction, relation, serverTime)
	case DeleteByte:
		return format.NewDelete(data, walStart, streamedTransaction, relation, serverTime)
	case LogicalByte:
		return format.NewLogicalMessage(data, walStart, streamedTransaction, serverTime)
	case TruncateByte:
		return format.NewTruncate(data, walStart, streamedTransaction, relation, serverTime)
	case BeginByte, CommitByte, OriginByte, TypeByte:
		// Transaction control and metadata messages - silently ignore
		return nil, nil
	case StreamStopByte, StreamAbortByte, StreamCommitByte:
		streamedTransaction = false
		return nil, nil
	case RelationByte:
		msg, err := format.NewRelation(data, walStart, streamedTransaction)
		if err == nil {
			relation[msg.OID] = msg
		}
		return msg, err
	case StreamStartByte:
		streamedTransaction = true
		return nil, nil
	default:
		return nil, errors.Wrap(ErrorByteNotSupported, string(data[0]))
	}
}
