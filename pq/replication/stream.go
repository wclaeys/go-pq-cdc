package replication

import (
	"context"
	"encoding/binary"
	goerrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
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
	ErrorSlotInUse    = errors.New("replication slot in use")
	ErrorNotConnected = errors.New("stream is not connected")
)

const (
	StandbyStatusUpdateByteID = 'r'
)

type ListenerFunc func(ack Acknowledger, message format.WALMessage)

type Acknowledger func(lsn pq.LSN) error

type Streamer interface {
	Connect(ctx context.Context) error
	Open(ctx context.Context) error
	Close(ctx context.Context)
	GetSystemInfo() *pq.IdentifySystemResult
	GetMetric() metric.Metric
	OpenFromSnapshotLSN()
	Acknowledge(lsn pq.LSN) error
}

type stream struct {
	conn                pq.Connection
	metric              metric.Metric
	sinkEnd             chan struct{}
	sinkStarted         chan struct{} // Signals when sink is actively receiving
	relation            map[uint32]*format.Relation
	messageCH           chan format.WALMessage
	listenerFunc        ListenerFunc
	system              *pq.IdentifySystemResult
	mu                  *sync.RWMutex
	acknowledger        func(pos pq.LSN) error
	config              config.Config
	lastXLogPos         pq.LSN
	snapshotLSN         pq.LSN
	closed              atomic.Bool
	openFromSnapshotLSN bool
}

func NewStream(ctx context.Context, dsn string, cfg config.Config, m metric.Metric, listenerFunc ListenerFunc) Streamer {
	stream := &stream{
		conn:         pq.NewConnectionTemplate(dsn),
		metric:       m,
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		messageCH:    make(chan format.WALMessage, 1000),
		listenerFunc: listenerFunc,
		lastXLogPos:  10,
		sinkStarted:  make(chan struct{}),
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

func (s *stream) Connect(ctx context.Context) error {
	if err := s.conn.Connect(ctx); err != nil {
		return errors.Wrap(err, "stream connection")
	}

	system, err := pq.IdentifySystem(ctx, s.conn)
	if err != nil {
		_ = s.conn.Close(ctx)
		return errors.Wrap(err, "identify system")
	}

	s.system = &system
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database:", system.Database)
	return nil
}

func (s *stream) Open(ctx context.Context) error {
	if s.conn.IsClosed() {
		return ErrorNotConnected
	}

	if err := s.setup(ctx); err != nil {
		s.sinkEnd <- struct{}{}

		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "55006" {
			return ErrorSlotInUse
		}
		return errors.Wrap(err, "replication setup")
	}

	go s.sink(ctx)

	go s.process(ctx)

	// Wait for sink goroutine to signal it's ready to receive messages.
	// This ensures no WAL records are missed due to goroutine scheduling.
	select {
	case <-s.sinkStarted:
		// Sink is ready
	case <-time.After(10 * time.Second):
		logger.Warn("timeout waiting for sink to start")
	case <-ctx.Done():
		return ctx.Err()
	}

	logger.Info("cdc stream started")

	return nil
}

func (s *stream) setup(ctx context.Context) error {
	replication := New(s.conn)

	replicationStartLsn := pq.LSN(2)
	if s.openFromSnapshotLSN {
		snapshotLSN, err := s.fetchSnapshotLSN(ctx)
		if err != nil {
			return errors.Wrap(err, "fetch snapshot LSN")
		}
		replicationStartLsn = snapshotLSN
	}

	if err := replication.Start(s.config.Publication.Name, s.config.Slot.Name, replicationStartLsn); err != nil {
		return err
	}

	if err := replication.Test(ctx); err != nil {
		return err
	}

	if s.openFromSnapshotLSN {
		logger.Info("replication started from snapshot LSN", "slot", s.config.Slot.Name)
	} else {
		logger.Info("replication started from restart LSN", "slot", s.config.Slot.Name)
	}

	return nil
}

//nolint:funlen
func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")

	var corruptedConn bool

	// Get underlying net.Conn for direct deadline manipulation.
	// This avoids the overhead of creating a new context for every message.
	netConn := s.conn.NetConn()
	if netConn == nil {
		logger.Error("failed to get underlying net.Conn")
		s.sinkEnd <- struct{}{}
		return
	}

	// Timeout for read operations - used to periodically check for shutdown
	// and send standby status updates.
	const readTimeout = 300 * time.Millisecond

	// Signal ready BEFORE first ReceiveMessage call.
	// This ensures we're in the receive loop before Open() returns.
	close(s.sinkStarted)

	// Next scheduled standby status update. Keep this time-driven so updates
	// are sent even when WAL is flowing continuously (i.e. no timeout occurs).
	nextStatus := time.Now().Add(readTimeout)

	// Cache the currently programmed deadline to avoid calling SetReadDeadline
	// on every message. SetReadDeadline is the expensive part, not time.Now().Add.
	var curDeadline time.Time

	for {
		//msgCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*300))
		//rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		//cancel()

		now := time.Now()

		// Periodically send standby status updates regardless of traffic.
		if !nextStatus.After(now) {
			err := SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos()))
			if err != nil {
				logger.Error("send stand by status update", "error", err)
				break
			}
			if logger.IsDebugEnabled() {
				logger.Debug("send stand by status update")
			}
			nextStatus = now.Add(readTimeout)
		}

		// Set deadline directly on the underlying connection.
		// This is much faster than creating a new context every iteration.
		//
		// Performance: avoid calling SetReadDeadline for every message. Only refresh
		// when the deadline actually changes (typically once per readTimeout).
		deadline := nextStatus
		if curDeadline.IsZero() || absDuration(curDeadline.Sub(deadline)) >= time.Millisecond {
			if err := netConn.SetReadDeadline(deadline); err != nil {
				logger.Error("failed to set read deadline", "error", err)
				corruptedConn = true
				break
			}
			curDeadline = deadline
		}

		// Use context.Background() to skip pgx's context watcher overhead.
		// The read deadline set above handles timeouts directly.
		rawMsg, err := s.conn.ReceiveMessage(context.Background())

		if err != nil {
			if s.closed.Load() {
				logger.Info("stream stopped")
				break
			}

			if pgconn.Timeout(err) {
				// Timeout is now just a wake-up mechanism; standby updates are time-driven above.
				continue
			}
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
			// If the primary requests a reply, send a standby status update immediately.
			//
			// Primary keepalive payload (after the type byte) is:
			//   int64 walEnd (big-endian)
			//   int64 serverTime (big-endian)
			//   byte  replyRequested (0/1)
			//
			// We only need replyRequested for behavior; parsing is allocation-free.
			replyRequested, ok := parsePrimaryKeepaliveReplyRequested(msg.Data[1:])
			if ok && replyRequested {
				err := SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos()))
				if err != nil {
					logger.Error("send stand by status update", "error", err)
					break
				}
				if logger.IsDebugEnabled() {
					logger.Debug("send stand by status update")
				}
				// Avoid immediately sending again in the time-driven section.
				nextStatus = time.Now().Add(readTimeout)
			}
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
			decodedMsg, err = message.New(xld.WALData, xld.WALStart, xld.ServerTime, s.relation)
			if err != nil {
				// Actual parsing error - log at error level
				logger.Error("wal data message parsing error", "error", err)
				continue
			}
			if decodedMsg == nil {
				// Control message (Begin, Commit, etc.) - silently skip
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

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// parsePrimaryKeepaliveReplyRequested parses only the replyRequested flag from the primary keepalive message
// payload (i.e., msg.Data[1:]). It returns (flag, true) if the payload is well-formed.
func parsePrimaryKeepaliveReplyRequested(payload []byte) (bool, bool) {
	// Expected length: 8 + 8 + 1 = 17 bytes.
	if len(payload) < 17 {
		return false, false
	}
	// walEnd := binary.BigEndian.Uint64(payload[0:8])
	_ = binary.BigEndian.Uint64(payload[0:8])
	// serverTime := binary.BigEndian.Uint64(payload[8:16])
	_ = binary.BigEndian.Uint64(payload[8:16])
	return payload[16] != 0, true
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

func (s *stream) SetSnapshotLSN(lsn pq.LSN) {
	s.snapshotLSN = lsn
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

func (s *stream) OpenFromSnapshotLSN() {
	s.openFromSnapshotLSN = true
}

// fetchSnapshotLSN queries the database to get the snapshot LSN from cdc_snapshot_job table
// Uses infinite retry with exponential backoff for resilience against transient database errors
func (s *stream) fetchSnapshotLSN(ctx context.Context) (pq.LSN, error) {
	logger.Info("fetching snapshot LSN from database", "slotName", s.config.Slot.Name)

	var snapshotLSN pq.LSN

	err := retry.Do(
		func() error {
			// Create a separate connection for querying metadata
			// Use regular DSN (not replication DSN) for normal SQL queries
			conn, err := pq.NewConnection(ctx, s.config.DSN())
			if err != nil {
				return errors.Wrap(err, "create connection for snapshot LSN query")
			}
			defer conn.Close(ctx)

			query := fmt.Sprintf(`
				SELECT snapshot_lsn, completed 
				FROM cdc_snapshot_job 
				WHERE slot_name = '%s'
			`, s.config.Slot.Name)

			resultReader := conn.Exec(ctx, query)
			results, err := resultReader.ReadAll()
			if err != nil {
				resultReader.Close()
				return errors.Wrap(err, "execute snapshot LSN query")
			}

			if err = resultReader.Close(); err != nil {
				return errors.Wrap(err, "close result reader")
			}

			if len(results) == 0 || len(results[0].Rows) == 0 {
				return retry.Unrecoverable(errors.New("no snapshot job found for slot: " + s.config.Slot.Name))
			}

			row := results[0].Rows[0]

			completed := string(row[1]) == "true" || string(row[1]) == "t"
			if !completed {
				return errors.New("snapshot job not completed yet for slot: " + s.config.Slot.Name)
			}

			lsnStr := string(row[0])
			if lsnStr == "" {
				return retry.Unrecoverable(errors.New("empty snapshot LSN result"))
			}

			snapshotLSN, err = pq.ParseLSN(lsnStr)
			if err != nil {
				return retry.Unrecoverable(errors.Wrap(err, "parse snapshot LSN: "+lsnStr))
			}

			return nil
		},
		retry.Attempts(0),                   // 0 means infinite retries
		retry.DelayType(retry.BackOffDelay), // Exponential backoff
		retry.OnRetry(func(n uint, err error) {
			logger.Error("error in snapshot LSN fetch, retrying",
				"attempt", n+1,
				"error", err,
				"slotName", s.config.Slot.Name)
		}),
	)
	if err != nil {
		return 0, errors.Wrap(err, "failed to fetch snapshot LSN")
	}

	logger.Info("fetched snapshot LSN from database", "slotName", s.config.Slot.Name, "snapshotLSN", snapshotLSN.String())
	return snapshotLSN, nil
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
