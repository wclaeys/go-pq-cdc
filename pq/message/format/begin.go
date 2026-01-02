package format

import (
	"encoding/binary"
	"time"

	"github.com/go-playground/errors"
	"github.com/wclaeys/go-pq-cdc/pq"
)

type Begin struct {
	CommitTime time.Time
	FinalLSN   pq.LSN
	Xid        uint32
}

func NewBegin(data []byte) (*Begin, error) {
	msg := &Begin{}
	if err := msg.decode(data); err != nil {
		return nil, err
	}
	return msg, nil
}

func (b *Begin) decode(data []byte) error {
	skipByte := 1

	if len(data) < 20 {
		return errors.Newf("begin message length must be at least 20 byte, but got %d", len(data))
	}

	b.FinalLSN = pq.LSN(binary.BigEndian.Uint64(data[skipByte:]))
	skipByte += 8
	b.CommitTime = time.Unix(int64(binary.BigEndian.Uint64(data[skipByte:])), 0)
	skipByte += 8
	b.Xid = binary.BigEndian.Uint32(data[skipByte:])

	return nil
}

// Implements the WALMessage interface
func (m *Begin) GetLSN() pq.LSN {
	return m.FinalLSN
}

// Implements the WALMessage interface
func (m *Begin) SetLSN(lsn pq.LSN) {
	m.FinalLSN = lsn
}
