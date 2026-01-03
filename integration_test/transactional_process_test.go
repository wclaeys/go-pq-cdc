package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	cdc "github.com/wclaeys/go-pq-cdc"
	"github.com/wclaeys/go-pq-cdc/config"
	"github.com/wclaeys/go-pq-cdc/pq/message/format"
	"github.com/wclaeys/go-pq-cdc/pq/replication"
)

func TestTransactionalProcess(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_transactional_process"

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	messageCh := make(chan any, 500)
	handlerFunc := func(ack replication.Acknowledger, walMessage format.WALMessage) {
		switch msg := walMessage.(type) {
		case *format.Insert, *format.Delete, *format.Update:
			messageCh <- msg
		}
		_ = ack(walMessage.GetLSN())
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	cfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Cleanup(func() {
		connector.Close()
		err = RestoreDB(ctx)
		assert.NoError(t, err)

		pool.Close()
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if !assert.NoError(t, connector.WaitUntilReady(waitCtx)) {
		t.FailNow()
	}
	cancel()

	t.Run("Start transactional operation and commit. Then check the messages and metrics", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		assert.NoError(t, err)

		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES (12, 'j*va is best')")
		assert.NoError(t, err)

		_, err = tx.Exec(ctx, "UPDATE books SET name = 'go is best' WHERE id = 12")
		assert.NoError(t, err)

		err = tx.Commit(ctx)
		assert.NoError(t, err)

		insertMessage := <-messageCh
		ins := insertMessage.(*format.Insert)
		idIndex, _ := ins.Relation.GetColumnIndexByName("id")
		nameIndex, _ := ins.Relation.GetColumnIndexByName("name")
		assert.Equal(t, int32(12), ins.TupleData[idIndex])
		assert.Equal(t, "j*va is best", ins.TupleData[nameIndex])
		updateMessage := <-messageCh
		up := updateMessage.(*format.Update)
		assert.Equal(t, int32(12), up.NewTupleData[idIndex])
		assert.Equal(t, "go is best", up.NewTupleData[nameIndex])

		updateMetric, _ := fetchUpdateOpMetric()
		insertMetric, _ := fetchInsertOpMetric()
		deleteMetric, _ := fetchDeleteOpMetric()
		assert.True(t, updateMetric == 1)
		assert.True(t, insertMetric == 1)
		assert.True(t, deleteMetric == 0)
	})

	t.Run("Start transactional operation and rollback. Then Delete book which id is 12. Then check the messages and metrics", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		assert.NoError(t, err)

		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES (13, 'j*va is best')")
		assert.NoError(t, err)

		_, err = tx.Exec(ctx, "UPDATE books SET name = 'go is best' WHERE id = 13")
		assert.NoError(t, err)

		err = tx.Rollback(ctx)
		assert.NoError(t, err)

		_, err = pool.Exec(ctx, "DELETE FROM books WHERE id = 12")
		assert.NoError(t, err)

		deleteMessage := <-messageCh
		del := deleteMessage.(*format.Delete)
		idIndexDel, _ := del.Relation.GetColumnIndexByName("id")
		assert.Equal(t, int32(12), del.OldTupleData[idIndexDel])

		updateMetric, _ := fetchUpdateOpMetric()
		insertMetric, _ := fetchInsertOpMetric()
		deleteMetric, _ := fetchDeleteOpMetric()
		assert.True(t, updateMetric == 1)
		assert.True(t, insertMetric == 1)
		assert.True(t, deleteMetric == 1)
	})
}
