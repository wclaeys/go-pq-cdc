package replication

import (
	"encoding/binary"
	"time"

	"github.com/go-playground/errors"

	"github.com/wclaeys/go-pq-cdc/pq"
)

// The server's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
// microSecFromUnixEpochToY2K is unix timestamp of 2000-01-01.
// Precompute the Postgres epoch (Y2K) in microseconds since Unix epoch.
var y2kUnixMicro = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro() // = 946684800000000

type XLogData struct {
	ServerTime   time.Time
	WALData      []byte
	WALStart     pq.LSN
	ServerWALEnd pq.LSN
}

func ParseXLogData(buf []byte) (XLogData, error) {
	if len(buf) < 24 {
		return XLogData{}, errors.Newf("XLogData must be at least 24 bytes, got %d", len(buf))
	}

	return XLogData{
		WALStart:     pq.LSN(binary.BigEndian.Uint64(buf)),
		ServerWALEnd: pq.LSN(binary.BigEndian.Uint64(buf[8:])),
		ServerTime:   pgTimeToTime(int64(binary.BigEndian.Uint64(buf[16:]))), //nolint:gosec // G115: bit-identical two's-complement reinterpretation
		WALData:      buf[24:],                                               // remaining bytes
	}, nil
}

// Convert "microseconds since Y2K" -> time.Time (UTC).
func pgTimeToTime(usSinceY2K int64) time.Time {
	return time.UnixMicro(y2kUnixMicro + usSinceY2K)
}
