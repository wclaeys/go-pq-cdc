package format

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

// helper to build an 'M' frame.
// layout (non-streamed): 'M' flags(1) lsn(8) prefix(nul) len(4) content
// layout (streamed):     'M' xid(4) flags(1) lsn(8) prefix(nul) len(4) content
func buildM(t *testing.T, streamed bool, xid uint32, flags byte, lsn uint64, prefix string, content []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	b.WriteByte('M')
	if streamed {
		_ = binary.Write(&b, binary.BigEndian, xid)
	}
	b.WriteByte(flags)
	_ = binary.Write(&b, binary.BigEndian, lsn)
	b.WriteString(prefix)
	b.WriteByte(0) // NUL
	//nolint:gosec // G115: len(content) is capped below 4GiB in this test; safe to cast to uint32.
	_ = binary.Write(&b, binary.BigEndian, uint32(len(content)))
	b.Write(content)
	return b.Bytes()
}

func TestNewLogicalMessage_NonStreamed_OK(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	content := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	raw := buildM(t, false, 0, 1, 0x0102030405060708, "cdc.orders", content)

	msg, err := NewLogicalMessage(raw, false, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !msg.Transactional {
		t.Errorf("Transactional = false, want true")
	}
	if msg.LSN != 0x0102030405060708 {
		t.Errorf("LSN = %x, want 0102030405060708", msg.LSN)
	}
	if msg.XID != 0 {
		t.Errorf("XID = %d, want 0", msg.XID)
	}
	if msg.Prefix != "cdc.orders" {
		t.Errorf("Prefix = %q, want cdc.orders", msg.Prefix)
	}
	if !bytes.Equal(msg.Content, content) {
		t.Errorf("Content = %x, want %x", msg.Content, content)
	}
	if !msg.MessageTime.Equal(now) {
		t.Errorf("MessageTime = %v, want %v", msg.MessageTime, now)
	}
}

func TestNewLogicalMessage_Streamed_OK(t *testing.T) {
	now := time.Unix(1700000001, 0).UTC()
	content := []byte("hello")
	raw := buildM(t, true, 42, 0, 0xAABBCCDDEEFF0011, "it.cdc", content)

	msg, err := NewLogicalMessage(raw, true, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Transactional {
		t.Errorf("Transactional = true, want false")
	}
	if msg.LSN != 0xAABBCCDDEEFF0011 {
		t.Errorf("LSN = %x, want AABBCCDDEEFF0011", msg.LSN)
	}
	if msg.XID != 42 {
		t.Errorf("XID = %d, want 42", msg.XID)
	}
	if msg.Prefix != "it.cdc" {
		t.Errorf("Prefix = %q, want it.cdc", msg.Prefix)
	}
	if string(msg.Content) != "hello" {
		t.Errorf("Content = %q, want %q", string(msg.Content), "hello")
	}
	if !msg.MessageTime.Equal(now) {
		t.Errorf("MessageTime = %v, want %v", msg.MessageTime, now)
	}
}

func TestNewLogicalMessage_Errors(t *testing.T) {
	now := time.Now()

	// 1) wrong tag
	if _, err := NewLogicalMessage([]byte("X"), false, now); err == nil {
		t.Error("expected error for invalid tag")
	}

	// 2) streamed but too short for xid
	{
		raw := []byte{'M', 0x00, 0x00, 0x01} // only 3 bytes of xid
		if _, err := NewLogicalMessage(raw, true, now); err == nil {
			t.Error("expected error for short xid")
		}
	}

	// 3) short header (flags+lsn)
	{
		raw := []byte{'M', 0x01} // flags only, missing lsn
		if _, err := NewLogicalMessage(raw, false, now); err == nil {
			t.Error("expected error for short header")
		}
	}

	// 4) unterminated prefix
	{
		var b bytes.Buffer
		b.WriteByte('M')
		b.WriteByte(1)                                    // flags
		_ = binary.Write(&b, binary.BigEndian, uint64(1)) // lsn
		b.WriteString("no-nul")                           // missing NUL
		if _, err := NewLogicalMessage(b.Bytes(), false, now); err == nil {
			t.Error("expected error for unterminated prefix")
		}
	}

	// 5) short content length
	{
		var b bytes.Buffer
		b.WriteByte('M')
		b.WriteByte(1)                                    // flags
		_ = binary.Write(&b, binary.BigEndian, uint64(1)) // lsn
		b.WriteString("pfx")
		b.WriteByte(0)
		_ = binary.Write(&b, binary.BigEndian, uint32(10)) // claims 10 bytes
		b.Write([]byte("12345"))                           // only 5
		if _, err := NewLogicalMessage(b.Bytes(), false, now); err == nil {
			t.Error("expected error for short content")
		}
	}
}
