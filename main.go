package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"sync"
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

type FileContext struct {
	fileno  uint32
	data    []byte
	state   []bool
	segSize uint16
}

func newRecvFileContext(header *Header, segSize uint16) *FileContext {
	return &FileContext{
		fileno:  header.Fileno,
		data:    make([]byte, header.TotalLength),
		state: make([]bool, (int(header.TotalLength) + int(segSize) - 1) / int(segSize)),
		segSize: segSize,
	}
}

func (f *FileContext) DataMsg(buf []byte, seqno uint32) []byte {
	header := Header{
		Type:         TypeDATA,
		HeaderLength: RobustPHeaderLen,
		Length:       0,
		Fileno:       f.fileno,
		Seqno:        seqno,
		TotalLength:  uint32(len(f.data)),
	}
	size := f.segSize
	if int(seqno)+int(size) > len(f.data) {
		size = uint16(len(f.data) - int(seqno))
	}
	header.Length = uint16(header.HeaderLength) + size

	header.Encode(buf)
	copy(buf[header.HeaderLength:header.Length], f.data[header.Seqno:])
	return buf[:header.Length]
}

func (f *FileContext) SaveData(header *Header, buf []byte) error {
	if f.fileno != header.Fileno {
		return fmt.Errorf("invalid fileno for save data")
	}
	stateno := header.Seqno / uint32(f.segSize)
	if !f.state[stateno] {
		copy(f.data[header.Seqno:header.Seqno+uint32(header.Length-uint16(header.HeaderLength))], buf[header.HeaderLength:])
		f.state[stateno] = true
	} else {
		// already ack
	}
	return nil
}

func (f *FileContext) IsCompleted() bool {
	for _, b := range f.state {
		if !b {
			return false
		}
	}
	return true
}

func sendFile(i uint32, data []byte, conn *net.UDPConn, mtu int) error {
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)

	f := FileContext{
		fileno:  i,
		data:    data,
		state:   nil,
		segSize: uint16(mss - RobustPHeaderLen),
	}

	buf := make([]byte, mss)

	for i := 0; i < len(data); i += int(f.segSize) {
		sendbuf := f.DataMsg(buf, uint32(i))
		_, err := conn.Write(sendbuf)
		if err != nil {
			return err
		}
	}
	return nil
}



func recvFile(conn *net.UDPConn, mtu int) ([]byte, error) {
	var header Header
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)
	segSize := uint16(mss - RobustPHeaderLen)
	buf := make([]byte, mss+1)

	var f *FileContext

	for {
		n, err := conn.Read(buf)
		if err != nil {
			return nil, err
		} else if n == len(buf) {
			return nil, errors.New("unexpected big size message")
		} else if n < RobustPHeaderLen {
			return nil, errors.New("unexpected small size message")
		}

		header.Parse(buf)

		switch header.Type {
		case TypeDATA:
			log.Println(header)
			log.Println("recv :", header.Seqno)
			if f == nil {
				f = newRecvFileContext(&header, segSize)
			}
			if err := f.SaveData(&header, buf); err != nil {
				return nil, err
			}
			// send ACK
			if f.IsCompleted() {
				log.Println("finished")
				return f.data, nil
			}

		default:
			return nil, errors.New("unexpected type")
		}
	}
}

func main() {
	srcaddr, err := net.ResolveUDPAddr("udp4", "169.254.251.212:19809")
	if err != nil {
		log.Panic(err)
	}
	destaddr, err := net.ResolveUDPAddr("udp4", "169.254.22.60:19809")
	if err != nil {
		log.Panic(err)
	}
	conn, err := net.DialUDP("udp4", srcaddr, destaddr)
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()
	log.Println("hello world")

	lconn, err := net.ListenUDP("udp4", destaddr)
	if err != nil {
		log.Panic(err)
	}

	data, err := ioutil.ReadFile("tmp/sample")
	if err != nil {
		log.Panic(err)
	}

	var wg sync.WaitGroup

	log.Println("allocated")

	wg.Add(2)

	// sender
	go func() {
		defer wg.Done()
		if err := sendFile(0, data, conn, 1500); err != nil {
			log.Panic(err)
		}
	}()

	// recver
	go func() {
		defer wg.Done()
		d, err := recvFile(lconn, 1500)
		if err != nil {
			log.Println(err)
		} else if bytes.Equal(d[:], data[:]) {
			log.Println("same data")
		} else {
			log.Println("not same data")
			log.Println(d[:10])
			log.Println(data[:10])
			log.Println(d[1024:1034])
			log.Println(data[1024:1034])
			log.Println(d[2048:2058])
			log.Println(data[2048:2058])
			log.Println(d[102400-481:])
			log.Println(data[102400-481:])
		}
	}()

	wg.Wait()
}
