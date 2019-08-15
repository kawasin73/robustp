package main

import (
	"encoding/binary"
	"fmt"
)

const (
	TypeDATA = iota + 1
	TypeACK
)

const (
	IPHeaderLen      = 20
	UDPHeaderLen     = 8
	RobustPHeaderLen = 16
)

type Header struct {
	Type         uint8
	HeaderLength uint8
	Length       uint16
	Fileno       uint32
	Seqno        uint32
	TotalLength  uint32
}

func (h *Header) String() string {
	var t string
	switch h.Type {
	case TypeDATA:
		t = "DATA"
	case TypeACK:
		t = "ACK"
	default:
		t = fmt.Sprintf("UNK(%d)", h.Type)
	}
	return fmt.Sprintf("{type: %q, headerlen: %d, length: %d, fileno: %d, seqno: %d, total: %d}", t,
		h.HeaderLength, h.Length, h.Fileno, h.Seqno, h.TotalLength)
}

func (h *Header) Parse(buf []byte) {
	h.Type = buf[0]
	h.HeaderLength = buf[1]
	h.Length = binary.BigEndian.Uint16(buf[2:])
	h.Fileno = binary.BigEndian.Uint32(buf[4:])
	h.Seqno = binary.BigEndian.Uint32(buf[8:])
	h.TotalLength = binary.BigEndian.Uint32(buf[12:])
}

func (h *Header) Encode(buf []byte) {
	buf[0] = h.Type
	buf[1] = h.HeaderLength
	binary.BigEndian.PutUint16(buf[2:], h.Length)
	binary.BigEndian.PutUint32(buf[4:], h.Fileno)
	binary.BigEndian.PutUint32(buf[8:], h.Seqno)
	binary.BigEndian.PutUint32(buf[12:], h.TotalLength)
}

type PartialAck struct {
	seqno  uint32
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
		seqno := binary.BigEndian.Uint32(buf)
		length := binary.BigEndian.Uint32(buf[4:])
		acks[i] = PartialAck{seqno: seqno, length: length}
	}
	return acks
}

func EncodePartialAck(buf []byte, header *Header, acks []PartialAck) {
	buf = buf[header.HeaderLength:]
	for _, ack := range acks {
		if len(buf) < 8 {
			break
		}
		binary.BigEndian.PutUint32(buf, ack.seqno)
		binary.BigEndian.PutUint32(buf[4:], ack.length)
		header.Length += 8
		buf = buf[8:]
	}
}
