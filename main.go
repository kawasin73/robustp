package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	log "github.com/kawasin73/robustp/logger"
)

type TransSegment struct {
	sendat  time.Time
	segment *FileSegment
	ack     bool
}

type ReadHandler interface {
	HandleRead(ctx context.Context, buf []byte, header *Header) error
}

func readThread(ctx context.Context, wg *sync.WaitGroup, conn *net.UDPConn, segSize uint16, handler ReadHandler) {
	defer wg.Done()

	var header Header
	buf := make([]byte, RobustPHeaderLen+segSize+1)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(time.Millisecond * 10)); err != nil {
			log.Panic(err)
		}
		n, err := conn.Read(buf)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				select {
				case <-ctx.Done():
					log.Info("shutdown read thread")
					return
				default:
					continue
				}
			}
			log.Panic(err)
		} else if n == len(buf) {
			log.Panic(fmt.Sprintf("unexpected big size message"))
		} else if n < RobustPHeaderLen {
			log.Panic(fmt.Sprintf("unexpected small size message"))
		}

		header.Parse(buf)

		log.Debug("recv :", &header)

		if err = handler.HandleRead(ctx, buf, &header); err != nil {
			log.Error("failed to handle read :", err)
		}
	}
}

type Sender struct {
	segSize     uint16
	chSendFile  chan *FileSegment
	chAck       chan AckMsg
	rtt         *RTTCollecter
	established bool
}

func createSender(ctx context.Context, wg *sync.WaitGroup, conn *net.UDPConn, mtu int, rtt *RTTCollecter, ctrl CongestionControlAlgorithm) *Sender {
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)
	segSize := uint16(mss - RobustPHeaderLen)
	s := &Sender{
		segSize:    segSize,
		chSendFile: make(chan *FileSegment),
		chAck:      make(chan AckMsg),
		rtt:        rtt,
	}
	wg.Add(2)
	go readThread(ctx, wg, conn, segSize, s)
	go s.sendThread(ctx, wg, conn, ctrl)
	return s
}

func (s *Sender) HandleRead(ctx context.Context, buf []byte, header *Header) error {
	switch header.Type {
	case TypeACK:
		partials := ParsePartialAck(buf[:header.Length], header)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.chAck <- AckMsg{header: *header, partials: partials}:
		}
		return nil

	case TypeACK_CONN:
		if s.established {
			return nil
		}
		s.established = true
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.chAck <- AckMsg{header: *header, partials: nil}:
		}
		return nil

	default:
		return fmt.Errorf("unexpected type for sender : %v", header)
	}
}

func (s *Sender) sendThread(ctx context.Context, wg *sync.WaitGroup, conn *net.UDPConn, ctrl CongestionControlAlgorithm) {
	defer wg.Done()
	var window []TransSegment
	buf := make([]byte, RobustPHeaderLen+s.segSize)

	timer := time.NewTimer(10 * time.Millisecond)

	var (
		chQueueSegment <-chan *FileSegment = s.chSendFile
		transHead      uint32
		nsent          int
		isTimer        bool
	)

	setTimer := func() {
		if !isTimer && len(window) > 0 {
			// window[0] must be ack == false
			timer.Reset(window[0].sendat.Add(s.rtt.RTO).Sub(time.Now()))
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
		window = append(window, TransSegment{sendat: now, segment: segment})
		nsent++

		// set timer
		setTimer()
		return nsent < ctrl.WindowSize()
	}

	// wait for connection established
establish:
	for {
		select {
		case <-ctx.Done():
			log.Info("shutdown sender thread")
			timer.Stop()
			return

		case <-s.chAck:
			// accept ACK_CONN
			timer.Stop()
			break establish

		case <-timer.C:
			// send CONN msg
			header := Header{
				Type:         TypeCONN,
				HeaderLength: RobustPHeaderLen,
				Length:       RobustPHeaderLen,
			}
			connbuf := EncodeHeaderMsg(buf, &header)
			if _, err := conn.Write(connbuf); err != nil {
				log.Panic(err)
			}

			// wait for ACK_CONN
			timer.Reset(10 * time.Millisecond)
		}
	}

	// start send files
	for {
		log.Debug("congestion size:", ctrl.WindowSize())
		select {
		case <-ctx.Done():
			log.Info("shutdown sender thread")
			timer.Stop()
			return

		case ack := <-s.chAck:
			log.Debug("recv ack : ", &ack)
			log.Debug("transHead :", transHead)
			log.Debug("window size:", len(window))
			log.Debug("nsent :", nsent)
			log.Debug("rto :", s.rtt.RTO)
			log.Debug("rtt :", s.rtt)

			// TODO: not get time everytime
			acktime := time.Now()

			for transHead < ack.header.TransId {
				// TODO: resend
				//ctrl.Add(CONG_LOSS|CONG_EARLY, item.sendat, 0)
			}

			// TODO: backup overflow segments

			// if transId is valid
			idxWindow := int(ack.header.TransId) - int(transHead)
			if idxWindow >= 0 && idxWindow < len(window) && !window[idxWindow].ack {
				// remove from window
				window[idxWindow].ack = true
				nsent--

				item := window[idxWindow]
				item.segment.Ack(&ack)

				rtt := acktime.Sub(item.sendat)
				ctrl.Add(CONG_SUCCESS, item.sendat, rtt)

				// save RTT
				log.Debug("rttd:", rtt)
				s.rtt.AddRTT(rtt)
			}

			// pop from window (vacuum)
			for len(window) > 0 && window[0].ack {
				window = window[1:]
				transHead++
			}

			if nsent < ctrl.WindowSize() {
				chQueueSegment = s.chSendFile
			} else {
				chQueueSegment = nil
			}

		case now := <-timer.C:
			// timer is disabled
			isTimer = false

			for len(window) > 0 && (window[0].sendat.Add(s.rtt.RTO).Before(now) || window[0].ack) {
				// pop item
				item := window[0]
				window = window[1:]
				transHead++

				if !item.ack {
					nsent--
					if !item.segment.IsCompleted() {
						log.Infof("timeout segment : %v", item)
						ctrl.Add(CONG_LOSS|CONG_TIMEOUT, item.sendat, 0)
						// resend
						sendItem(item.segment)
					} else {
						ctrl.Add(CONG_SUCCESS|CONG_NOACK, item.sendat, now.Sub(item.sendat))
					}
				}
				// TODO: backup overflow segments
			}
			// restart chQueue
			if nsent < ctrl.WindowSize() {
				chQueueSegment = s.chSendFile
			} else {
				chQueueSegment = nil
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
	segSize     uint16
	chRecvFile  chan *FileContext
	files       map[uint32]*FileContext
	established time.Time
	// TODO: have conn?
	conn *net.UDPConn
}

func createReceiver(ctx context.Context, wg *sync.WaitGroup, conn *net.UDPConn, mtu int) *Receiver {
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)
	segSize := uint16(mss - RobustPHeaderLen)
	r := &Receiver{
		segSize:    segSize,
		chRecvFile: make(chan *FileContext),
		files:      make(map[uint32]*FileContext),
		conn:       conn,
	}
	wg.Add(1)
	go readThread(ctx, wg, conn, segSize, r)
	return r
}

func (r *Receiver) HandleRead(ctx context.Context, buf []byte, header *Header) error {
	switch header.Type {
	case TypeDATA:
		f, ok := r.files[header.Fileno]
		if !ok {
			f = newRecvFileContext(header, r.segSize)
			r.files[header.Fileno] = f
		}
		if !f.IsAllCompleted() {
			if err := f.SaveData(header, buf[:header.Length]); err != nil {
				log.Panic(err)
			}
			if f.IsAllCompleted() {
				r.chRecvFile <- f
				// TODO: remove file context
				// それ以降に Data が送られて来ないように ACK + FIN によるコネクションを閉じる考え方が必要。
			}
		}
		ackbuf := f.AckMsg(buf, header.TransId)
		if _, err := r.conn.Write(ackbuf); err != nil {
			log.Panic(fmt.Errorf("send ack msg : %v", err))
		}
		log.Debug("recv state :", f.state)
		return nil

	case TypeCONN:
		now := time.Now()
		if now.Sub(r.established) < 20*time.Millisecond {
			// already established
			return nil
		}
		// response ACK_CONN
		header.Type = TypeACK_CONN
		ackbuf := EncodeHeaderMsg(buf, header)
		if _, err := r.conn.Write(ackbuf); err != nil {
			log.Panic(fmt.Errorf("send ack conn msg : %v", err))
		}
		r.established = now
		return nil

	default:
		return fmt.Errorf("unexpected type for receiver : %v", header)
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

	data, err := ioutil.ReadFile("tmp/sample")
	if err != nil {
		log.Panic(err)
	}

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rtt := newRTTCollecter(30, &DoubleRTO{})

	// sender
	sender := createSender(ctx, &wg, sendConn, 1500, rtt, NewSimpleControl(260))

	receiver := createReceiver(ctx, &wg, recvConn, 1500)

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

	time.Sleep(100 * time.Millisecond)

	log.Info("shutdown...")

	cancel()
	wg.Wait()
}
