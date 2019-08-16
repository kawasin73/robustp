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

func (s *TransSegment) String() string {
	return fmt.Sprintf("{sendat: %v, segment: %v, ack: %v}", s.sendat.Format(time.RFC3339Nano), s.segment, s.ack)
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

type WindowManager struct {
	window    []TransSegment
	ctrl      CongestionControlAlgorithm
	transHead uint32
	nsent     int
	ChTimer   chan time.Time
	timer     *time.Timer
	isTimer   bool
	rtt       *RTTCollecter
}

func NewWindowManager(ctrl CongestionControlAlgorithm, rtt *RTTCollecter) *WindowManager {
	// init timer
	timer := time.NewTimer(0)
	timer.Stop()
	select {
	case <-timer.C:
	default:
	}
	return &WindowManager{
		ctrl:  ctrl,
		timer: timer,
		rtt:   rtt,
	}
}

func (w *WindowManager) Push(segment *FileSegment) uint32 {
	transId := w.transHead + uint32(len(w.window))
	now := time.Now()

	// add window
	w.window = append(w.window, TransSegment{sendat: now, segment: segment})
	w.nsent++

	if !w.isTimer {
		w.timer.Reset(w.rtt.RTO)
		w.isTimer = true
	}
	return transId
}

func (w *WindowManager) AckSegment(ack *AckMsg) ([]*FileSegment, bool) {
	acktime := time.Now()
	var segs []*FileSegment

	//for w.transHead < ack.header.TransId {
	//	// TODO: resend append retry buffer
	//	//ctrl.Add(CONG_LOSS|CONG_EARLY, item.sendat, 0)
	//}

	var ok bool

	// if transId is valid
	idxWindow := int(ack.header.TransId) - int(w.transHead)
	if idxWindow >= 0 && idxWindow < len(w.window) && !w.window[idxWindow].ack {
		// remove from window
		w.window[idxWindow].ack = true
		w.nsent--
		ok = true

		item := w.window[idxWindow]
		item.segment.Ack(ack)

		rtt := acktime.Sub(item.sendat)
		w.ctrl.Add(CONG_SUCCESS, item.sendat, rtt)

		// save RTT
		log.Debug("rttd:", rtt)
		w.rtt.AddRTT(rtt)
	}

	// pop from window (vacuum)
	for len(w.window) > 0 && w.window[0].ack {
		w.window = w.window[1:]
		w.transHead++
	}

	return segs, ok
}

func (w *WindowManager) CheckTimeout(now time.Time) ([]*FileSegment, int) {
	var noack int
	w.isTimer = false

	var segs []*FileSegment
	for len(w.window) > 0 && (w.window[0].sendat.Add(w.rtt.RTO).Before(now) || w.window[0].ack) {
		item := w.window[0]
		w.window = w.window[1:]
		w.transHead++

		if !item.ack {
			w.nsent--
			if !item.segment.IsCompleted() {
				log.Infof("timeout segment : %v", &item)
				w.ctrl.Add(CONG_LOSS|CONG_TIMEOUT, item.sendat, 0)
				// resend
				segs = append(segs, item.segment)
			} else {
				noack++
				w.ctrl.Add(CONG_SUCCESS|CONG_NOACK, item.sendat, now.Sub(item.sendat))
			}
		}
	}

	// reactivate timer
	if len(w.window) > 0 {
		// window[0] must be ack == false
		w.timer.Reset(w.window[0].sendat.Add(w.rtt.RTO).Sub(time.Now()))
		w.isTimer = true
	}

	return segs, noack
}

func (w *WindowManager) RestSize() int {
	return w.ctrl.WindowSize() - w.nsent
}

type SegmentEnqueuer struct {
	chQueue     chan *FileSegment
	ChQueue     chan *FileSegment
	retryBuffer []*FileSegment
	window      *WindowManager
}

func NewSegmentEnqueuer(chQueue chan *FileSegment, window *WindowManager) *SegmentEnqueuer {
	return &SegmentEnqueuer{
		chQueue: chQueue,
		ChQueue: chQueue,
		window:  window,
	}
}

func (q *SegmentEnqueuer) Retry(retry []*FileSegment) {
	q.retryBuffer = append(q.retryBuffer, retry...)
	if len(q.retryBuffer) > 0 || q.window.RestSize() <= 0 {
		q.ChQueue = nil
	} else {
		q.ChQueue = q.chQueue
	}
}

func (q *SegmentEnqueuer) Pop() *FileSegment {
	if len(q.retryBuffer) == 0 {
		return nil
	}
	segment := q.retryBuffer[0]
	q.retryBuffer = q.retryBuffer[1:]
	if len(q.retryBuffer) > 0 || q.window.RestSize() <= 0 {
		q.ChQueue = nil
	} else {
		q.ChQueue = q.chQueue
	}
	return segment
}

type Sender struct {
	segSize     uint16
	chAck       chan AckMsg
	chQueue     chan *FileSegment
	enqueue     *SegmentEnqueuer
	established bool
}

func createSender(ctx context.Context, wg *sync.WaitGroup, conn *net.UDPConn, mtu int, window *WindowManager) *Sender {
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)
	segSize := uint16(mss - RobustPHeaderLen)
	chQueue := make(chan *FileSegment, 100)
	s := &Sender{
		segSize: segSize,
		chQueue: chQueue,
		enqueue: NewSegmentEnqueuer(chQueue, window),
		chAck:   make(chan AckMsg),
	}
	wg.Add(2)
	go readThread(ctx, wg, conn, segSize, s)
	go s.sendThread(ctx, wg, conn, window)
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

func waitForEstablish(ctx context.Context, buf []byte, conn *net.UDPConn, chAck chan AckMsg) error {
	timer := time.NewTimer(10 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			log.Info("shutdown sender thread")
			timer.Stop()
			return ctx.Err()

		case <-chAck:
			// accept ACK_CONN
			timer.Stop()
			return nil

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
}

func (s *Sender) sendThread(ctx context.Context, wg *sync.WaitGroup, conn *net.UDPConn, window *WindowManager) {
	defer wg.Done()
	buf := make([]byte, RobustPHeaderLen+s.segSize)

	// wait for connection established
	if err := waitForEstablish(ctx, buf, conn, s.chAck); err != nil {
		log.Panic(err)
	}

	var (
		nsuccess, ntimeout, nresend, nnoack int
	)

	defer func() {
		log.Infof("sender : success segment : %d", nsuccess)
		log.Infof("sender : timeout segment : %d", ntimeout)
		log.Infof("sender : resend  segment : %d", nresend)
		log.Infof("sender : noack   segment : %d", nnoack)
	}()

	sendSegment := func(segment *FileSegment) {
		transId := window.Push(segment)
		log.Debug("send data:", transId)
		sendbuf := segment.PackMsg(buf, transId)
		_, err := conn.Write(sendbuf)
		if err != nil {
			log.Panic(err)
		}
	}

	// start send files
	for {
		log.Debug("congestion size:", window.ctrl.WindowSize())
		log.Debug("nsent :", window.nsent)
		select {
		case <-ctx.Done():
			log.Info("shutdown sender thread")
			return

		case ack := <-s.chAck:
			// debug log
			log.Debug("recv ack : ", &ack)
			log.Debug("transHead :", window.transHead)
			if len(window.window) > 0 {
				log.Debug("window head :", &window.window[0])
			} else {
				log.Debug("no window")
			}
			log.Debug("window actual size:", len(window.window))
			log.Debug("rto :", window.rtt.RTO)
			log.Debug("rtt :", window.rtt)

			if ack.header.Type != TypeACK {
				log.Info("accept not ACK msg :", ack)
				continue
			}

			retry, ok := window.AckSegment(&ack)

			// for statistics
			if ok {
				nsuccess++
			}
			nresend += len(retry)

			for window.RestSize() > 0 {
				if segment := s.enqueue.Pop(); segment != nil {
					sendSegment(segment)
				} else {
					break
				}
			}

			for ; len(retry) > 0 && window.RestSize() > 0; retry = retry[1:] {
				// resend
				sendSegment(retry[0])
			}

			if len(retry) > 0 {
				s.enqueue.Retry(retry)
				log.Debugf("window size shrink and segments overflow. retry: %d, window: %d", len(retry), window.ctrl.WindowSize())
			}

		case now := <-window.timer.C:
			retry, noack := window.CheckTimeout(now)

			// for statistics
			nnoack += noack
			ntimeout += len(retry)

			for window.RestSize() > 0 {
				if segment := s.enqueue.Pop(); segment != nil {
					sendSegment(segment)
				} else {
					break
				}
			}

			for ; len(retry) > 0 && window.RestSize() > 0; retry = retry[1:] {
				sendSegment(retry[0])
			}

			if len(retry) > 0 {
				s.enqueue.Retry(retry)
				log.Debugf("window size shrink and segments overflow. retry: %d, window: %d", window.ctrl.WindowSize())
			}

		case segment := <-s.enqueue.ChQueue:
			sendSegment(segment)
		}
	}
}

func (s *Sender) sendFile(ctx context.Context, i uint32, data []byte) error {
	f := newSendFileContext(i, data, s.segSize)
	for offset := 0; offset < len(f.data); offset += int(f.segSize) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.chQueue <- &FileSegment{file: f, offset: uint32(offset)}:
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
	log.SetLevel(log.LvInfo)
	go func() {
		log.Error(http.ListenAndServe("localhost:6060", nil))
	}()
	srcaddr, err := net.ResolveUDPAddr("udp4", "169.254.251.212:19810")
	if err != nil {
		log.Panic(err)
	}
	destaddr, err := net.ResolveUDPAddr("udp4", "169.254.22.60:19810")
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
	window := NewWindowManager(NewSimpleControl(10000), rtt)

	// sender
	sender := createSender(ctx, &wg, sendConn, 1500, window)

	receiver := createReceiver(ctx, &wg, recvConn, 1500)

	const (
		filenum = 100
	)
	go func() {
		for i := 0; i < filenum; i++ {
			sender.sendFile(ctx, uint32(i), data)
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
