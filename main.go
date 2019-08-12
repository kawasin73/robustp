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
	"time"
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

type FileContext struct {
	fileno  uint32
	data    []byte
	state   []bool
	segSize uint16

	completed int
}

func newSendFileContext(fileno uint32, data []byte, segSize uint16) *FileContext {
	return &FileContext{
		fileno:  fileno,
		data:    data,
		state:   make([]bool, (len(data)+int(segSize)-1)/int(segSize)),
		segSize: segSize,
	}
}

func newRecvFileContext(header *Header, segSize uint16) *FileContext {
	return &FileContext{
		fileno:  header.Fileno,
		data:    make([]byte, header.TotalLength),
		state:   make([]bool, (int(header.TotalLength)+int(segSize)-1)/int(segSize)),
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

func (f *FileContext) AckMsg(buf []byte) []byte {
	header := Header{
		Type:         TypeACK,
		HeaderLength: RobustPHeaderLen,
		Length:       RobustPHeaderLen,
		Fileno:       f.fileno,
		Seqno:        uint32(f.completed * int(f.segSize)),
		TotalLength:  uint32(len(f.data)),
	}
	header.Encode(buf)
	return buf[:header.Length]
}

func (f *FileContext) SaveData(header *Header, buf []byte) error {
	if f.fileno != header.Fileno {
		return fmt.Errorf("invalid fileno for save data")
	}
	stateno := int(header.Seqno) / int(f.segSize)

	if !f.state[stateno] {
		if buf != nil {
			copy(f.data[header.Seqno:header.Seqno+uint32(header.Length-uint16(header.HeaderLength))], buf[header.HeaderLength:])
		}
		f.state[stateno] = true

		// update completed stateno
		completed := f.completed
		for ; completed < len(f.state) && f.state[completed]; completed++ {
		}
		f.completed = completed
	} else {
		// already ack
	}
	return nil
}

func (f *FileContext) AckData(header *Header) error {
	if f.fileno != header.Fileno {
		return fmt.Errorf("invalid fileno for save data")
	}
	stateno := int(header.Seqno) / int(f.segSize)

	// update completed stateno
	if f.completed < stateno {
		for i := f.completed; i < stateno; i++ {
			f.state[i] = true
		}
		f.completed = stateno
	}
	return nil
}

func (f *FileContext) IsCompleted(seqno uint32) bool {
	return f.state[seqno/uint32(f.segSize)]
}

func (f *FileContext) IsAllCompleted() bool {
	return f.completed == len(f.state)
}

type WaitItem struct {
	sentat time.Time
	fctx   *FileContext
	seqno  uint32
}

func sendFile(i uint32, data []byte, conn *net.UDPConn, mtu int) error {
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)

	f := newSendFileContext(i, data, uint16(mss-RobustPHeaderLen))

	var timers []*WaitItem

	buf := make([]byte, mss)

	for i := 0; i < len(data); i += int(f.segSize) {
		sendbuf := f.DataMsg(buf, uint32(i))
		_, err := conn.Write(sendbuf)
		if err != nil {
			return err
		}
		timers = append(timers, &WaitItem{
			sentat: time.Now(),
			fctx:   f,
			seqno:  uint32(i),
		})
	}

	chAck := make(chan Header)

	go recvFile(conn, mtu, chAck)

	rto := time.Second

	timer := time.NewTimer(timers[0].sentat.Add(rto).Sub(time.Now()))

	for {
		select {
		case ack := <-chAck:
			log.Println("recv ack : ", &ack)
			if err := f.AckData(&ack); err != nil {
				return fmt.Errorf("recv ack : %v", err)
			}
			if f.IsAllCompleted() {
				log.Println("send finished")
				return nil
			}

		case now := <-timer.C:
			for len(timers) > 0 && timers[0].sentat.Before(now) {
				// pop item
				item := timers[0]
				timers = timers[1:]

				if !item.fctx.IsCompleted(item.seqno) {
					log.Println("timeout : item.seqno :", item.seqno)
					// resend
					sendbuf := item.fctx.DataMsg(buf, item.seqno)
					_, err := conn.Write(sendbuf)
					if err != nil {
						return err
					}

					// re-queue wait item
					item.sentat = time.Now()
					timers = append(timers, item)
				}
			}
			if len(timers) > 0 {
				// reset timer
				timer.Reset(timers[0].sentat.Add(rto).Sub(time.Now()))
			}
		}
	}

	return nil
}

func recvFile(conn *net.UDPConn, mtu int, chAck chan<- Header) ([]byte, error) {
	var header Header
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)
	segSize := uint16(mss - RobustPHeaderLen)
	buf := make([]byte, mss+1)

	var lost bool

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

		log.Println("recv :", &header)

		switch header.Type {
		case TypeDATA:
			if f == nil {
				f = newRecvFileContext(&header, segSize)
			}
			if err := f.SaveData(&header, buf); err != nil {
				return nil, err
			}
			// send ACK
			//chAck <- header

			if !lost && header.Seqno+uint32(header.Length) > header.TotalLength {
				lost = true
			} else {
				ackbuf := f.AckMsg(buf)
				if _, err := conn.Write(ackbuf); err != nil {
					return nil, fmt.Errorf("send ack msg : %v", err)
				}
				if f.IsAllCompleted() {
					log.Println("recv finished")
					return f.data, nil
				}
			}

		case TypeACK:
			chAck <- header

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

	sendConn, err := net.DialUDP("udp4", srcaddr, destaddr)
	if err != nil {
		log.Panic(err)
	}
	defer sendConn.Close()

	recvConn, err := net.DialUDP("udp4", destaddr, srcaddr)
	if err != nil {
		log.Panic(err)
	}
	defer recvConn.Close()
	log.Println("hello world")

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
		if err := sendFile(0, data, sendConn, 1500); err != nil {
			log.Panic(err)
		}
	}()

	// recver
	go func() {
		defer wg.Done()
		chAck := make(chan Header)
		//go func() {
		//	for {
		//		ack := <-chAck
		//
		//	}
		//}()
		d, err := recvFile(recvConn, 1500, chAck)
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
