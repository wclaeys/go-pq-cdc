package format

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wclaeys/go-pq-cdc/pq/message/tuple"
)

func TestDelete_New(t *testing.T) {
	// The following data represents a delete event:
	// For the old tuple: id=645, name=foo
	data := []byte{
		68, 0, 0, 64, 6, // header
		79, 0, 2, // old tuple type, 2 columns
		116, 0, 0, 0, 3, 54, 52, 53, // old id: type=116, len=3, data="645"
		116, 0, 0, 0, 3, 102, 111, 111, // old name: type=116, len=3, data="foo"
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
	msg, err := NewDelete(data, 0, false, rel, now, true)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Delete{
		OID:          16390,
		XID:          0,
		OldTupleType: 79,
		OldTupleData: &tuple.Data{
			ColumnNumber: 2,
			Columns: tuple.DataColumns{
				&tuple.DataColumn{
					DataType: 116,
					Length:   3,
					Data:     []byte("645"),
				},
				&tuple.DataColumn{
					DataType: 116,
					Length:   3,
					Data:     []byte("foo"),
				},
			},
			SkipByte: 24,
		},
		OldDecoded: map[string]any{
			"id":   int32(645),
			"name": "foo",
		},
		TableNamespace: "public",
		TableName:      "t",
		MessageTime:    now,
	}

	assert.Equal(t, expected, msg)
}
