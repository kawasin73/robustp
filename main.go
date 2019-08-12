package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	log "github.com/kawasin73/robustp/logger"
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

type Sender struct {
	segSize    uint16
	chSendFile chan *FileContext
}

func createSender(conn *net.UDPConn, mtu int) *Sender {
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)
	segSize := uint16(mss - RobustPHeaderLen)
	s := &Sender{
		segSize:    segSize,
		chSendFile: make(chan *FileContext),
	}
	chAck := make(chan Header)
	go s.recvThread(conn, chAck)
	go s.sendThread(conn, chAck)
	return s
}

func (s *Sender) recvThread(conn *net.UDPConn, chAck chan Header) {
	var header Header
	buf := make([]byte, RobustPHeaderLen+s.segSize+1)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Panic(err)
		} else if n == len(buf) {
			log.Panic(fmt.Sprintf("unexpected big size message"))
		} else if n < RobustPHeaderLen {
			log.Panic(fmt.Sprintf("unexpected small size message"))
		}

		header.Parse(buf)

		log.Debug("recv :", &header)

		switch header.Type {
		case TypeACK:
			chAck <- header

		default:
			log.Panic(fmt.Errorf("unexpected type"))
		}
	}
}

func (s *Sender) sendThread(conn *net.UDPConn, chAck chan Header) {
	var timers []*WaitItem
	buf := make([]byte, RobustPHeaderLen+s.segSize)
	rto := time.Second
	files := make(map[uint32]*FileContext)
	timer := time.NewTimer(0)
	timer.Stop()

	for {
		select {
		case ack := <-chAck:
			log.Debug("recv ack : ", &ack)
			f, ok := files[ack.Fileno]
			if !ok {
				log.Errorf("error recv ack : fileno %d is not found\n", ack.Fileno)
				continue
			}
			if err := f.AckData(&ack); err != nil {
				log.Panic(fmt.Errorf("recv ack : %v", err))
			}
			if f.IsAllCompleted() {
				log.Infof("send finished fileno %d\n", f.fileno)
			}

		case now := <-timer.C:
			for len(timers) > 0 && timers[0].sentat.Before(now) {
				// pop item
				item := timers[0]
				timers = timers[1:]

				if !item.fctx.IsCompleted(item.seqno) {
					log.Debug("timeout : item.seqno :", item.seqno)
					// resend
					sendbuf := item.fctx.DataMsg(buf, item.seqno)
					_, err := conn.Write(sendbuf)
					if err != nil {
						log.Panic(err)
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

		case f := <-s.chSendFile:
			restartTimer := len(timers) == 0
			files[f.fileno] = f
			for i := 0; i < len(f.data); i += int(f.segSize) {
				sendbuf := f.DataMsg(buf, uint32(i))
				_, err := conn.Write(sendbuf)
				if err != nil {
					log.Panic(err)
					return
				}
				timers = append(timers, &WaitItem{
					sentat: time.Now(),
					fctx:   f,
					seqno:  uint32(i),
				})
			}
			if restartTimer {
				// reset timer
				timer.Reset(timers[0].sentat.Add(rto).Sub(time.Now()))
			}
		}
	}
}

func (s *Sender) sendFile(i uint32, data []byte) error {
	f := newSendFileContext(i, data, s.segSize)
	s.chSendFile <- f
	log.Debug("queue ", i)
	return nil
}

type Receiver struct {
	segSize    uint16
	chRecvFile chan *FileContext
}

func createReceiver(conn *net.UDPConn, mtu int) *Receiver {
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)
	segSize := uint16(mss - RobustPHeaderLen)
	r := &Receiver{
		segSize:    segSize,
		chRecvFile: make(chan *FileContext),
	}
	go r.recvThread(conn)
	return r
}

func (r *Receiver) recvThread(conn *net.UDPConn) {
	var header Header
	buf := make([]byte, RobustPHeaderLen+r.segSize+1)

	files := make(map[uint32]*FileContext)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Panic(err)
		} else if n == len(buf) {
			log.Panic(fmt.Sprintf("unexpected big size message"))
		} else if n < RobustPHeaderLen {
			log.Panic(fmt.Sprintf("unexpected small size message"))
		}

		header.Parse(buf)

		log.Debug("recv :", &header)

		switch header.Type {
		case TypeDATA:
			f, ok := files[header.Fileno]
			if !ok {
				f = newRecvFileContext(&header, r.segSize)
				files[header.Fileno] = f
			}
			if !f.IsAllCompleted() {
				if err := f.SaveData(&header, buf); err != nil {
					log.Panic(err)
				}
				if f.IsAllCompleted() {
					r.chRecvFile <- f
				}
			}
			ackbuf := f.AckMsg(buf)
			if _, err := conn.Write(ackbuf); err != nil {
				log.Panic(fmt.Errorf("send ack msg : %v", err))
			}
			log.Debug("recv state :", f.state)

		default:
			log.Panic(fmt.Errorf("unexpected type"))
		}
	}
}

func main() {
	log.SetLevel(log.LvInfo)
	go func() {
		log.Error(http.ListenAndServe("localhost:6060", nil))
	}()
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
	log.Info("hello world")

	data, err := ioutil.ReadFile("tmp/sample")
	if err != nil {
		log.Panic(err)
	}

	log.Info("allocated")

	// sender
	sender := createSender(sendConn, 1500)

	receiver := createReceiver(recvConn, 1500)

	const (
		filenum = 100
	)
	go func() {
		for i := 0; i < filenum; i++ {
			sender.sendFile(uint32(i), data)
		}
	}()

	for i := 0; i < filenum; i++ {
		f := <-receiver.chRecvFile
		if bytes.Equal(f.data[:], data[:]) {
			log.Info("same data", f.fileno)
		} else {
			log.Info("not same data")
			log.Debug(f.data[:10])
			log.Debug(data[:10])
			log.Debug(f.data[1024:1034])
			log.Debug(data[1024:1034])
			log.Debug(f.data[2048:2058])
			log.Debug(data[2048:2058])
			log.Debug(f.data[102400-481:])
			log.Debug(data[102400-481:])
		}
		if err := ioutil.WriteFile(fmt.Sprintf("tmp/file%d", f.fileno), f.data, os.ModePerm); err != nil {
			log.Panic(err)
		}
	}
}
