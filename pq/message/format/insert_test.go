package format

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wclaeys/go-pq-cdc/pq/message/tuple"
)

func TestInsert_New(t *testing.T) {
	// The following data represents an insert event:
	// For the new tuple: id=605, name=foo
	data := []byte{
		73, 0, 0, 64, 6, // header
		78, 0, 2, // new tuple type, 2 columns
		116, 0, 0, 0, 3, 54, 48, 53, // new id: type=116, len=3, data="605"
		116, 0, 0, 0, 3, 102, 111, 111, // new name: type=116, len=3, data="foo"
	}

	rel := map[uint32]*Relation{
		16390: {
			OID:           16390,
			XID:           0,
			Namespace:     "public",
			Name:          "t",
			ReplicaID:     100,
			ColumnNumbers: 2,
			Columns: []tuple.RelationColumn{
				{
					Flags:        1,
					Name:         "id",
					DataType:     23,
					TypeModifier: 4294967295,
				},
				{
					Flags:        0,
					Name:         "name",
					DataType:     25,
					TypeModifier: 4294967295,
				},
			},
		},
	}

	now := time.Now()
	msg, err := NewInsert(data, 0, false, rel, now)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Insert{
		OID:            16390,
		XID:            0,
		TupleData:      map[string]any{"id": int32(605), "name": "foo"},
		TableNamespace: "public",
		TableName:      "t",
		MessageTime:    now,
	}

	assert.Equal(t, expected, msg)
}
