package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	cdc "github.com/wclaeys/go-pq-cdc"
	"github.com/wclaeys/go-pq-cdc/pq/message/format"
	"github.com/wclaeys/go-pq-cdc/pq/replication"
)

// This payload mirrors what the trigger emits below.
type msgPayload struct {
	Op      string `json:"op"`
	Schema  string `json:"schema"`
	Table   string `json:"table"`
	ID      int64  `json:"id"`
	OldName string `json:"old_name"`
	NewName string `json:"new_name"`
}

func TestLogicalMessage(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_logical_message"

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
		case *format.Insert, *format.Delete, *format.Update, *format.LogicalMessage:
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
	t.Run("Test logical message creation & streaming", func(t *testing.T) {
		pgExec(ctx, postgresConn, `
CREATE OR REPLACE FUNCTION it_emit_msg() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  payload bytea;
BEGIN
  payload := convert_to(
    jsonb_build_object(
      'op','u',
      'schema', TG_TABLE_SCHEMA,
      'table',  TG_TABLE_NAME,
      'id',         OLD.id,
      'old_name', OLD.name,
      'new_name', NEW.name
    )::text, 'UTF8');
  PERFORM pg_logical_emit_message(true, 'it.cdc', payload);
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS it_emit_msg_u ON public.books;
CREATE TRIGGER it_emit_msg_u
AFTER UPDATE ON public.books
FOR EACH ROW EXECUTE FUNCTION it_emit_msg();
`)
		assert.NoError(t, err)

		// Insert a row to have something to update.
		books := CreateBooks(1)
		for _, b := range books {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO books(id, name) VALUES(%d, '%s')", b.ID, b.Name))
			assert.NoError(t, err)
		}
		for range 1 {
			<-messageCh
		}

		// Execute an update
		for i, b := range books {
			b.ID = i + 1
			b.Name = "updated-" + b.Name
			books[i] = b
			err = pgExec(ctx, postgresConn, fmt.Sprintf("UPDATE books SET name = '%s' WHERE id = %d", b.Name, b.ID))
			assert.NoError(t, err)
		}

		for i := range 2 {
			m := <-messageCh
			if i == 0 {
				// First we get the Update event.
				assert.Equal(t, books[i].Map(), m.(*format.Update).NewTupleData)
				continue
			}
			if i == 1 {
				// Then we get the LogicalMessage event
				assert.Equal(t, "it.cdc", m.(*format.LogicalMessage).Prefix)

				var p msgPayload
				if err := json.Unmarshal(m.(*format.LogicalMessage).Content, &p); err != nil {
					t.Errorf("decode content: %v", err)
					return
				}

				// Verify payload content
				if p.Op != "u" || p.Table != "books" || p.OldName == p.NewName || p.NewName != "updated-"+p.OldName {
					t.Errorf("unexpected payload: %+v", p)
					return
				}
			}
		}

		metricUpdate, _ := fetchUpdateOpMetric()
		assert.True(t, metricUpdate == 1)
		metricLogicalMsg, _ := fetchLogicalMessageOpMetric()
		assert.True(t, metricLogicalMsg == 1)
	})
}
