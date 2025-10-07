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

func TestTruncate(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_truncate_operation"

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
		case *format.Insert, *format.Truncate:
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

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if !assert.NoError(t, connector.WaitUntilReady(waitCtx)) {
		t.FailNow()
	}
	cancel()
	t.Run("Test truncate operation", func(t *testing.T) {
		// Insert a row to have something to truncate.
		books := CreateBooks(1)
		for _, b := range books {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO books(id, name) VALUES(%d, '%s')", b.ID, b.Name))
			assert.NoError(t, err)
		}
		for range 1 {
			<-messageCh
		}

		// Truncate the table.
		err = pgExec(ctx, postgresConn, "TRUNCATE TABLE books")
		assert.NoError(t, err)

		// Validate the receival of the Truncate event.
		truncateMessage := <-messageCh
		assert.Equal(t, "books", truncateMessage.(*format.Truncate).Relations[0].TableName)

		metricTruncate, _ := fetchTruncateOpMetric()
		assert.True(t, metricTruncate == 1)
	})
}
