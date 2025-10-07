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

	// Build body for truncate event:
	// nrels=2 (int32), opts=CASCADE|RESTART(3), OID(111), OID(222)
	// Wire format:
	//   'T' [Int32 nrels] [Int8 options] [Int32 OID] [Int32 OID]
	var body bytes.Buffer
	be32(&body, 2)       // nrels: number of relations
	body.WriteByte(0x03) // options: CASCADE(1) | RESTART IDENTITY(2)
	be32(&body, 111)     // OID for table a
	be32(&body, 222)     // OID for table b
	now := time.Unix(1, 0)

	// Final buffer: 'T' + body
	buf := append([]byte{'T'}, body.Bytes()...)
	tr, err := format.NewTruncate(buf, 0, false, relMap, now)
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

	// Build body for truncate event with streamed XID and unknown relation:
	// Wire format:
	//   'T' [Int32 XID] [Int32 nrels] [Int8 options] [Int32 OID] [Int32 OID]
	// OID(999) is not in relMap, should error
	var body bytes.Buffer
	be32(&body, 12345)   // XID
	be32(&body, 2)       // nrels
	body.WriteByte(0x00) // options: none
	be32(&body, 111)     // OID for table a
	be32(&body, 999)     // OID for unknown table

	buf := append([]byte{'T'}, body.Bytes()...)
	_, err := format.NewTruncate(buf, 0, true, relMap, time.Now())
	if err == nil {
		t.Fatalf("expected error for unknown relation")
	}
}
