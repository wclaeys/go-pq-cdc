package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	cdc "github.com/wclaeys/go-pq-cdc"
	"github.com/wclaeys/go-pq-cdc/config"
	"github.com/wclaeys/go-pq-cdc/pq/message/format"
	"github.com/wclaeys/go-pq-cdc/pq/replication"
)

func TestCopyProtocol(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_copy_protocol"

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	messageCh := make(chan format.WALMessage)
	totalCounter := atomic.Int64{}
	handlerFunc := func(ack replication.Acknowledger, walMessage format.WALMessage) {
		switch walMessage.(type) {
		case *format.Insert, *format.Delete, *format.Update:
			totalCounter.Add(1)
			messageCh <- walMessage
		}
	}

	cdc2Cfg := cdcCfg
	cdc2Cfg.Metric.Port = 8085
	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	connector2, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	cfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Cleanup(func() {
		pool.Close()
		connector2.Close()
		assert.NoError(t, RestoreDB(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if !assert.NoError(t, connector.WaitUntilReady(waitCtx)) {
		t.FailNow()
	}
	cancel()

	go connector2.Start(ctx)

	t.Run("Insert 30 book to table with Copy protocol. Then stop the consumer after 16th message processed", func(t *testing.T) {
		entries := make([][]any, 30)
		books := CreateBooks(30)

		for i, user := range books {
			entries[i] = []any{user.ID, user.Name}
		}

		_, err = pool.CopyFrom(
			ctx,
			pgx.Identifier{"books"},
			[]string{"id", "name"},
			pgx.CopyFromRows(entries),
		)
		if err != nil {
			t.Errorf("error copying into %s table: %v", "books", err)
		}

		idIndex := -1
		for {
			m := <-messageCh
			if v, ok := m.(*format.Insert); ok {
				if idIndex == -1 {
					idIndex, _ = v.Relation.GetColumnIndexByName("id")
				}
				if v.TupleData[idIndex].(int32) == 16 {
					connector.Close()
					break
				}
			}

			assert.NoError(t, connector.Acknowledge(m.GetLSN()))
		}
	})

	t.Run("Run CDC again. Then check message count after all messages consumed", func(t *testing.T) {
		waitCtx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		if !assert.NoError(t, connector2.WaitUntilReady(waitCtx)) {
			t.FailNow()
		}
		cancel()

		idIndex := -1
		for {
			m := <-messageCh
			if v, ok := m.(*format.Insert); ok {
				if idIndex == -1 {
					idIndex, _ = v.Relation.GetColumnIndexByName("id")
				}
				if v.TupleData[idIndex].(int32) == 30 {
					break
				}
			}
		}
	})
}
