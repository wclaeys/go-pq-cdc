package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	cdc "github.com/wclaeys/go-pq-cdc"
	"github.com/wclaeys/go-pq-cdc/pq/message/format"
	"github.com/wclaeys/go-pq-cdc/pq/publication"
	"github.com/wclaeys/go-pq-cdc/pq/replication"
)

func TestReplicaIdentityDefault(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_replica_identity_default"
	cdcCfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityDefault

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

	defer func() {
		connector.Close()
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	}()

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if !assert.NoError(t, connector.WaitUntilReady(waitCtx)) {
		t.FailNow()
	}
	cancel()

	t.Run("should return old value is nil when update message received", func(t *testing.T) {
		books := CreateBooks(10)
		for _, b := range books {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO books(id, name) VALUES(%d, '%s')", b.ID, b.Name))
			assert.NoError(t, err)
		}

		for range 10 {
			<-messageCh
		}

		booksNew := CreateBooks(5)
		for i, b := range booksNew {
			b.ID = i + 1
			booksNew[i] = b
			err = pgExec(ctx, postgresConn, fmt.Sprintf("UPDATE books SET name = '%s' WHERE id = %d", b.Name, b.ID))
			assert.NoError(t, err)
		}

		idIndex := -1
		for i := range 5 {
			m := <-messageCh
			if idIndex == -1 {
				idIndex, _ = m.(*format.Update).Relation.GetColumnIndexByName("id")
			}
			assert.Equal(t, booksNew[i].Array(), m.(*format.Update).NewTupleData)
			value, err := m.(*format.Update).GetDecodedOldValue(idIndex)
			assert.NoError(t, err)
			assert.Nil(t, value)
		}
	})
}

func TestReplicaIdentityFull(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_replica_identity_full"
	cdcCfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityFull

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

	t.Cleanup(func() {
		connector.Close()
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if !assert.NoError(t, connector.WaitUntilReady(waitCtx)) {
		t.FailNow()
	}
	cancel()

	t.Run("should return new value and old value when update message received", func(t *testing.T) {
		books := CreateBooks(10)
		for _, b := range books {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO books(id, name) VALUES(%d, '%s')", b.ID, b.Name))
			assert.NoError(t, err)
		}

		for range 10 {
			<-messageCh
		}

		booksNew := CreateBooks(5)
		for i, b := range booksNew {
			b.ID = i + 1
			booksNew[i] = b
			err = pgExec(ctx, postgresConn, fmt.Sprintf("UPDATE books SET name = '%s' WHERE id = %d", b.Name, b.ID))
			assert.NoError(t, err)
		}

		for i := range 5 {
			m := <-messageCh
			assert.Equal(t, booksNew[i].Array(), m.(*format.Update).NewTupleData)
			assert.Equal(t, books[i].Array(), m.(*format.Update).OldTupleData)
		}
	})
}
