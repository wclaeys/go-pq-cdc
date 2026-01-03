package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	cdc "github.com/wclaeys/go-pq-cdc"
	"github.com/wclaeys/go-pq-cdc/pq/message/format"
	"github.com/wclaeys/go-pq-cdc/pq/replication"
)

// go test -race -p=1 -v -run=TestBasicFunctionalitySanityTest
func TestBasicFunctionalitySanityTest(t *testing.T) {
	fmt.Println("sanity test")
}

func TestBasicFunctionality(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_basic_functionality"

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

	t.Run("Insert 10 book to table. Then check messages and metric", func(t *testing.T) {
		books := CreateBooks(10)
		for _, b := range books {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO books(id, name) VALUES(%d, '%s')", b.ID, b.Name))
			assert.NoError(t, err)
		}

		for i := range 10 {
			m := <-messageCh
			assert.Equal(t, books[i].Array(), m.(*format.Insert).TupleData)
		}

		metric, _ := fetchInsertOpMetric()
		assert.True(t, metric == 10)
	})

	t.Run("Update 5 book on table. Then check messages and metric", func(t *testing.T) {
		books := CreateBooks(5)
		for i, b := range books {
			b.ID = i + 1
			books[i] = b
			err = pgExec(ctx, postgresConn, fmt.Sprintf("UPDATE books SET name = '%s' WHERE id = %d", b.Name, b.ID))
			assert.NoError(t, err)
		}

		for i := range 5 {
			m := <-messageCh
			assert.Equal(t, books[i].Array(), m.(*format.Update).NewTupleData)
		}

		metric, _ := fetchUpdateOpMetric()
		assert.True(t, metric == 5)
	})

	t.Run("Delete 5 book from table. Then check messages and metric", func(t *testing.T) {
		for i := range 5 {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("DELETE FROM books WHERE id = %d", i+1))
			assert.NoError(t, err)
		}

		idIndex := -1
		for i := range 5 {
			m := <-messageCh
			if idIndex == -1 {
				idIndex, _ = m.(*format.Delete).Relation.GetColumnIndexByName("id")
			}
			assert.Equal(t, int32(i+1), m.(*format.Delete).OldTupleData[idIndex])
		}

		metric, _ := fetchDeleteOpMetric()
		assert.True(t, metric == 5)
	})
}
