package replication

import (
	"context"
	"encoding/binary"
	goerrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/wclaeys/go-pq-cdc/config"
	"github.com/wclaeys/go-pq-cdc/internal/metric"
	"github.com/wclaeys/go-pq-cdc/internal/slice"
	"github.com/wclaeys/go-pq-cdc/logger"
	"github.com/wclaeys/go-pq-cdc/pq"
	"github.com/wclaeys/go-pq-cdc/pq/message"
	"github.com/wclaeys/go-pq-cdc/pq/message/format"
)

var (
	ErrorSlotInUse = errors.New("replication slot in use")
)

const (
	StandbyStatusUpdateByteID = 'r'
)

type ListenerFunc func(ack Acknowledger, message format.WALMessage)

type Acknowledger func(lsn pq.LSN) error

type Streamer interface {
	Open(ctx context.Context) error
	Close(ctx context.Context)
	GetSystemInfo() *pq.IdentifySystemResult
	GetMetric() metric.Metric
	Acknowledge(lsn pq.LSN) error
}

type stream struct {
	conn         pq.Connection
	metric       metric.Metric
	system       *pq.IdentifySystemResult
	relation     map[uint32]*format.Relation
	messageCH    chan format.WALMessage
	listenerFunc ListenerFunc
	sinkEnd      chan struct{}
	mu           *sync.RWMutex
	acknowledger func(pos pq.LSN) error
	config       config.Config
	lastXLogPos  pq.LSN
	closed       atomic.Bool
}

func NewStream(ctx context.Context, conn pq.Connection, cfg config.Config, m metric.Metric, system *pq.IdentifySystemResult, listenerFunc ListenerFunc) Streamer {
	stream := &stream{
		conn:         conn,
		metric:       m,
		system:       system,
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		messageCH:    make(chan format.WALMessage, 1000),
		listenerFunc: listenerFunc,
		lastXLogPos:  10,
		sinkEnd:      make(chan struct{}, 1),
		mu:           &sync.RWMutex{},
	}
	stream.acknowledger = stream.createAcknowledger(ctx)
	return stream
}

func (s *stream) Acknowledge(lsn pq.LSN) error {
	return s.acknowledger(lsn)
}

func (s *stream) createAcknowledger(ctx context.Context) Acknowledger {
	return func(pos pq.LSN) error {
		s.system.UpdateXLogPos(pos)
		if logger.IsDebugEnabled() {
			logger.Debug("send stand by status update", "xLogPos", pos.String())
		}
		return SendStandbyStatusUpdate(ctx, s.conn, uint64(s.system.LoadXLogPos()))
	}
}

func (s *stream) Open(ctx context.Context) error {
	if err := s.setup(ctx); err != nil {
		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "55006" {
			return ErrorSlotInUse
		}
		return errors.Wrap(err, "replication setup")
	}

	go s.sink(ctx)

	go s.process(ctx)

	logger.Info("cdc stream started")

	return nil
}

func (s *stream) setup(ctx context.Context) error {
	replication := New(s.conn)

	if err := replication.Start(s.config.Publication.Name, s.config.Slot.Name); err != nil {
		return err
	}

	if err := replication.Test(ctx); err != nil {
		return err
	}

	logger.Info("replication started", "slot", s.config.Slot.Name)

	return nil
}

//nolint:funlen
func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")

	var corruptedConn bool
	timeout := 300 * time.Millisecond

	for {
		if ctx.Err() != nil {
			return // fast exit on cancellation/shutdown
		}

		msgCtx, cancel := context.WithTimeout(ctx, timeout)
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		cancel()

		if err != nil {
			if s.closed.Load() {
				logger.Info("stream stopped")
				break
			}

			// Fast-path: just a read timeout? send standby status and continue.
			if pgconn.Timeout(err) {
				err = SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos()))
				if err != nil {
					logger.Error("send stand by status update", "error", err)
					break
				}
				if logger.IsDebugEnabled() {
					logger.Debug("send stand by status update")
				}
				continue
			}
			// Other errors
			logger.Error("receive message error", "error", err)
			corruptedConn = true
			break
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			res, _ := errMsg.MarshalJSON()
			logger.Error("receive postgres wal error: " + string(res))
			continue
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			logger.Warn(fmt.Sprintf("received unexpected message: %T", rawMsg))
			continue
		}

		var xld XLogData

		switch msg.Data[0] {
		case message.PrimaryKeepaliveMessageByteID:
			continue
		case message.XLogDataByteID:
			xld, err = ParseXLogData(msg.Data[1:])
			if err != nil {
				logger.Error("parse xLog data", "error", err)
				continue
			}

			if logger.IsDebugEnabled() {
				logger.Debug("wal received", "walData", string(xld.WALData), "walDataByte", slice.ConvertToInt(xld.WALData), "walStart", xld.WALStart, "walEnd", xld.ServerWALEnd, "serverTime", xld.ServerTime)
			}

			s.metric.SetCDCLatency(max(time.Since(xld.ServerTime), time.Duration(0)).Nanoseconds())

			var decodedMsg format.WALMessage
			decodedMsg, err = message.New(xld.WALData, xld.WALStart, xld.ServerTime, s.relation, s.config.Message)
			if err != nil || decodedMsg == nil {
				if logger.IsDebugEnabled() {
					logger.Debug("wal data message parsing error", "error", err)
				}
				continue
			}

			s.messageCH <- decodedMsg
		}
	}

	// Teardown
	s.sinkEnd <- struct{}{}
	if !s.closed.Load() {
		s.Close(ctx)
		if corruptedConn {
			panic("corrupted connection")
		}
	}
}

var _baseTime = time.Now()

func (s *stream) process(ctx context.Context) {
	logger.Info("postgres message process started")

	acknowledger := s.createAcknowledger(ctx)

	for {
		msg, ok := <-s.messageCH
		if !ok {
			break
		}

		switch msg.(type) {
		case *format.Insert:
			s.metric.InsertOpIncrement(1)
		case *format.Delete:
			s.metric.DeleteOpIncrement(1)
		case *format.Update:
			s.metric.UpdateOpIncrement(1)
		case *format.Truncate:
			s.metric.TruncateOpIncrement(1)
		case *format.LogicalMessage:
			s.metric.LogicalMessageOpIncrement(1)
		}

		start := time.Since(_baseTime).Nanoseconds()
		s.listenerFunc(acknowledger, msg)
		s.metric.SetProcessLatency(time.Since(_baseTime).Nanoseconds() - start)
	}
}

func (s *stream) Close(ctx context.Context) {
	s.closed.Store(true)

	<-s.sinkEnd
	if !isClosed(s.sinkEnd) {
		close(s.sinkEnd)
	}
	logger.Info("postgres message sink stopped")

	if !s.conn.IsClosed() {
		_ = s.conn.Close(ctx)
		logger.Info("postgres connection closed")
	}
}

func (s *stream) GetSystemInfo() *pq.IdentifySystemResult {
	return s.system
}

func (s *stream) GetMetric() metric.Metric {
	return s.metric
}

func (s *stream) UpdateXLogPos(l pq.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastXLogPos < l {
		s.lastXLogPos = l
	}
}

func (s *stream) LoadXLogPos() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastXLogPos
}

func SendStandbyStatusUpdate(_ context.Context, conn pq.Connection, walWritePosition uint64) error {
	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, nowPgTime())
	data = append(data, 0)

	cd := &pgproto3.CopyData{Data: data}
	buf, err := cd.Encode(nil)
	if err != nil {
		return err
	}

	return conn.Frontend().SendUnbufferedEncodedCopyData(buf)
}

func AppendUint64(buf []byte, n uint64) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[wp:], n)
	return buf
}

// Fast path for "now"
func nowPgTime() uint64 {
	mu := time.Now().UnixMicro() - y2kUnixMicro
	if mu < 0 {
		return 0
	}
	return uint64(mu)
}

func isClosed[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}
