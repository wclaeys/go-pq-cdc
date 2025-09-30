package format_test

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/wclaeys/go-pq-cdc/pq/message/format"
)

func be32(b *bytes.Buffer, v uint32) { _ = binary.Write(b, binary.BigEndian, v) }

func TestNewTruncate_NoStreamedXID(t *testing.T) {
	// relation map with two tables
	relA := &format.Relation{Namespace: "public", Name: "a", OID: 111}
	relB := &format.Relation{Namespace: "public", Name: "b", OID: 222}
	relMap := map[uint32]*format.Relation{
		111: relA, 222: relB,
	}

	// Build body: nrels=2 (int32), opts=CASCADE|RESTART(3), OID(111), OID(222)
	var body bytes.Buffer
	be32(&body, 2)       // nrels
	body.WriteByte(0x03) // options: 1|2
	be32(&body, 111)     // oid a
	be32(&body, 222)     // oid b
	now := time.Unix(1, 0)

	buf := append([]byte{'T'}, body.Bytes()...)
	tr, err := format.NewTruncate(buf, false, relMap, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tr.Cascade || !tr.RestartIdentity {
		t.Fatalf("options not parsed: %#v", tr)
	}
	if len(tr.Relations) != 2 {
		t.Fatalf("want 2 relations, got %d", len(tr.Relations))
	}
	if tr.Relations[0].TableName != "a" || tr.Relations[1].TableName != "b" {
		t.Fatalf("bad names: %#v", tr.Relations)
	}
}

func TestNewTruncate_StreamedXIDAndUnknownRel(t *testing.T) {
	relA := &format.Relation{Namespace: "public", Name: "a", OID: 111}
	relMap := map[uint32]*format.Relation{111: relA}

	// Body: XID(4), nrels=2, opts=0, OID(111), OID(999) -> should error on 999
	var body bytes.Buffer
	be32(&body, 12345) // XID
	be32(&body, 2)     // nrels
	body.WriteByte(0x00)
	be32(&body, 111)
	be32(&body, 999)

	buf := append([]byte{'T'}, body.Bytes()...)
	_, err := format.NewTruncate(buf, true, relMap, time.Now())
	if err == nil {
		t.Fatalf("expected error for unknown relation")
	}
}
