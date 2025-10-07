package format

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wclaeys/go-pq-cdc/pq/message/tuple"
)

func TestUpdate_New(t *testing.T) {
	// The following data represents an update event:
	// For the old tuple: id=53, name=bar2
	// For the new tuple: id=53, name=bar5
	data := []byte{
		85, 0, 0, 64, 6, // header
		79, 0, 2, // old tuple type, 2 columns
		116, 0, 0, 0, 2, 53, 51, // old id: type=116, len=2, data="53"
		116, 0, 0, 0, 4, 98, 97, 114, 50, // old name: type=116, len=4, data="bar2"
		78, 0, 2, // new tuple type, 2 columns
		116, 0, 0, 0, 2, 53, 51, // new id: type=116, len=2, data="53"
		116, 0, 0, 0, 4, 98, 97, 114, 53, // new name: type=116, len=4, data="bar5"
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
	msg, err := NewUpdate(data, 0, false, rel, now)
	require.NoError(t, err)

	expected := &Update{
		OID: 16390,
		XID: 0,
		NewTupleData: map[string]any{
			"id":   int32(53),
			"name": "bar5",
		},
		OldTupleType: 79,
		OldTupleData: map[string]any{
			"id":   int32(53),
			"name": "bar2",
		},
		TableNamespace: "public",
		TableName:      "t",
		MessageTime:    now,
	}

	assert.Equal(t, expected, msg)
}
