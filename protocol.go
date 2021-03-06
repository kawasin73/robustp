package main

import (
	"encoding/binary"
	"fmt"
)

const (
	TypeDATA = iota + 1
	TypeACK
	TypeCONN
	TypeACK_CONN
)

const (
	IPHeaderLen      = 20
	UDPHeaderLen     = 8
	RobustPHeaderLen = 20
)

type Header struct {
	Type         uint8
	HeaderLength uint8
	Length       uint16
	TransId      uint32
	Fileno       uint32
	Offset       uint32
	TotalLength  uint32
}

func (h *Header) String() string {
	var t string
	switch h.Type {
	case TypeDATA:
		t = "DATA"
	case TypeACK:
		t = "ACK"
	case TypeCONN:
		t = "CONN"
	case TypeACK_CONN:
		t = "ACK_CONN"
	default:
		t = fmt.Sprintf("UNK(%d)", h.Type)
	}
	return fmt.Sprintf("{type: %q, headerlen: %d, length: %d, transid: %d, fileno: %d, offset: %d, total: %d}", t,
		h.HeaderLength, h.Length, h.TransId, h.Fileno, h.Offset, h.TotalLength)
}

func (h *Header) Parse(buf []byte) {
	h.Type = buf[0]
	h.HeaderLength = buf[1]
	h.Length = binary.BigEndian.Uint16(buf[2:])
	h.Fileno = binary.BigEndian.Uint32(buf[4:])
	h.Offset = binary.BigEndian.Uint32(buf[8:])
	h.TotalLength = binary.BigEndian.Uint32(buf[12:])
	h.TransId = binary.BigEndian.Uint32(buf[16:])
}

func (h *Header) Encode(buf []byte) {
	buf[0] = h.Type
	buf[1] = h.HeaderLength
	binary.BigEndian.PutUint16(buf[2:], h.Length)
	binary.BigEndian.PutUint32(buf[4:], h.Fileno)
	binary.BigEndian.PutUint32(buf[8:], h.Offset)
	binary.BigEndian.PutUint32(buf[12:], h.TotalLength)
	binary.BigEndian.PutUint32(buf[16:], h.TransId)
}

type PartialAck struct {
	offset uint32
	length uint32
}

type AckMsg struct {
	header   Header
	partials []PartialAck
}

func (ack *AckMsg) String() string {
	return fmt.Sprintf("{%v, %v}", &ack.header, ack.partials)
}

func ParsePartialAck(buf []byte, header *Header) []PartialAck {
	buf = buf[header.HeaderLength:header.Length]
	n := len(buf) / 8
	acks := make([]PartialAck, n)
	for i := 0; i < n; i++ {
		offset := binary.BigEndian.Uint32(buf)
		length := binary.BigEndian.Uint32(buf[4:])
		acks[i] = PartialAck{offset: offset, length: length}
	}
	return acks
}

func EncodePartialAck(buf []byte, header *Header, acks []PartialAck) {
	buf = buf[header.HeaderLength:]
	for _, ack := range acks {
		if len(buf) < 8 {
			break
		}
		binary.BigEndian.PutUint32(buf, ack.offset)
		binary.BigEndian.PutUint32(buf[4:], ack.length)
		header.Length += 8
		buf = buf[8:]
	}
}

func EncodeHeaderMsg(buf []byte, header *Header) []byte {
	header.Encode(buf)
	return buf[:RobustPHeaderLen]
}
