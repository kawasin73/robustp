package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	log "github.com/kawasin73/robustp/logger"
)

type TransSegment struct {
	sentat  time.Time
	segment *FileSegment
	ack     bool
}

type Sender struct {
	segSize    uint16
	chSendFile chan *FileSegment
	rtt        *RTTCollecter
}

func createSender(conn *net.UDPConn, mtu int, rtt *RTTCollecter) *Sender {
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)
	segSize := uint16(mss - RobustPHeaderLen)
	s := &Sender{
		segSize:    segSize,
		chSendFile: make(chan *FileSegment),
		rtt:        rtt,
	}
	chAck := make(chan AckMsg)
	go s.recvThread(conn, chAck)
	go s.sendThread(conn, chAck)
	return s
}

func (s *Sender) recvThread(conn *net.UDPConn, chAck chan AckMsg) {
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
			partials := ParsePartialAck(buf, &header)
			chAck <- AckMsg{header: header, partials: partials}

		default:
			log.Panic(fmt.Errorf("unexpected type"))
		}
	}
}

func (s *Sender) sendThread(conn *net.UDPConn, chAck chan AckMsg) {
	var window []TransSegment
	buf := make([]byte, RobustPHeaderLen+s.segSize)

	timer := time.NewTimer(0)
	timer.Stop()

	var (
		chQueueSegment <-chan *FileSegment = s.chSendFile
		transHead      uint32
		windowSize     = 260
		nsent          int
		isTimer        bool
	)

	setTimer := func() {
		if !isTimer && len(window) > 0 {
			// window[0] must be ack == false
			timer.Reset(window[0].sentat.Add(s.rtt.RTO).Sub(time.Now()))
			isTimer = true
		}
	}

	sendItem := func(segment *FileSegment) bool {
		transId := transHead + uint32(len(window))
		log.Debug("send data:", transId)
		sendbuf := segment.PackMsg(buf, transId)
		now := time.Now()
		_, err := conn.Write(sendbuf)
		if err != nil {
			log.Panic(err)
		}

		// add window
		window = append(window, TransSegment{sentat: now, segment: segment})
		nsent++

		// set timer
		setTimer()
		return nsent < windowSize
	}

	for {
		select {
		case ack := <-chAck:
			log.Debug("recv ack : ", ack)
			log.Debug("transHead :", transHead)
			log.Debug("window size:", len(window))
			log.Debug("nsent :", nsent)
			log.Debug("rto :", s.rtt.RTO)
			log.Debug("rtt :", s.rtt)

			// TODO: not get time everytime
			acktime := time.Now()

			// TODO: validate trans id
			for transHead < ack.header.TransId {
				// TODO: resend
			}

			// if transId is valid
			idxWindow := int(ack.header.TransId) - int(transHead)
			if idxWindow >= 0 && idxWindow < len(window) && !window[idxWindow].ack {
				// remove from window
				window[idxWindow].ack = true
				nsent--

				// TODO: save RTT
				item := window[idxWindow]
				item.segment.Ack(&ack)
				log.Debug("rttd:", acktime.Sub(item.sentat))
				s.rtt.AddRTT(acktime.Sub(item.sentat))
			}

			// pop from window (vacuum)
			for len(window) > 0 && window[0].ack {
				window = window[1:]
				transHead++
			}

			if nsent < windowSize {
				chQueueSegment = s.chSendFile
			}

		case now := <-timer.C:
			// timer is disabled
			isTimer = false

			for len(window) > 0 && (window[0].sentat.Add(s.rtt.RTO).Before(now) || window[0].ack) {
				// pop item
				item := window[0]
				window = window[1:]
				transHead++

				if !item.ack {
					nsent--
					if !item.segment.IsCompleted() {
						log.Infof("timeout segment : %v", item)
						// resend
						sendItem(item.segment)
					}
				}
			}
			// restart chQueue
			if nsent < windowSize {
				chQueueSegment = s.chSendFile
			}

			// reactivate timer
			setTimer()

		case segment := <-chQueueSegment:
			if !sendItem(segment) {
				chQueueSegment = nil
			}
		}
	}
}

func (s *Sender) sendFile(i uint32, data []byte) error {
	f := newSendFileContext(i, data, s.segSize)
	for offset := 0; offset < len(f.data); offset += int(f.segSize) {
		s.chSendFile <- &FileSegment{
			file:   f,
			offset: uint32(offset),
		}
	}
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
					// TODO: remove file context
					// それ以降に Data が送られて来ないように ACK + FIN によるコネクションを閉じる考え方が必要。
				}
			}
			ackbuf := f.AckMsg(buf, header.TransId)
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
	log.SetLevel(log.LvDebug)
	go func() {
		log.Error(http.ListenAndServe("localhost:6060", nil))
	}()
	srcaddr, err := net.ResolveUDPAddr("udp4", "localhost:19809")
	if err != nil {
		log.Panic(err)
	}
	destaddr, err := net.ResolveUDPAddr("udp4", "localhost:19810")
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

	rtt := newRTTCollecter(30, &DoubleRTO{})

	// sender
	sender := createSender(sendConn, 1500, rtt)

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
