package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	log "github.com/kawasin73/robustp/logger"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"time"
)

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
	// TODO: something smells...
	q.Touch()
}

func (q *SegmentEnqueuer) Pop() *FileSegment {
	if len(q.retryBuffer) == 0 {
		// TODO: something smells...
		q.Touch()
		return nil
	}
	segment := q.retryBuffer[0]
	q.retryBuffer = q.retryBuffer[1:]
	// TODO: something smells...
	q.Touch()
	return segment
}

func (q *SegmentEnqueuer) Touch() {
	if len(q.retryBuffer) > 0 || q.window.RestSize() <= 0 {
		q.ChQueue = nil
	} else {
		q.ChQueue = q.chQueue
	}
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
		chAck:   make(chan AckMsg, 100),
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

	log.Info("connection established")

	var (
		nsuccess, ntimeout, nresend, nnoack int
	)

	report := func() {
		log.Infof("sender : success segment : %d", nsuccess)
		log.Infof("sender : timeout segment : %d", ntimeout)
		log.Infof("sender : resend  segment : %d", nresend)
		log.Infof("sender : noack   segment : %d", nnoack)
		log.Info("congestion size:", window.ctrl.WindowSize())
		log.Info("window actual size:", len(window.window))
		log.Info("nsent :", window.nsent)
		log.Info("rest size:", window.RestSize())
		log.Info("retry buffer size:", len(s.enqueue.retryBuffer))
	}

	defer func() {
		report()
	}()

	sendSegment := func(segment *FileSegment) {
		transId := window.Push(segment)
		log.Debug("send data:", transId)
		sendbuf := segment.PackMsg(buf, transId)
		_, err := conn.Write(sendbuf)
		if err != nil {
			log.Panic(err)
		}
		s.enqueue.Touch()
	}

	reportTick := time.NewTicker(5 * time.Second)

	// start send files
	for {
		log.Debug("congestion size:", window.ctrl.WindowSize())
		log.Debug("window actual size:", len(window.window))
		log.Debug("nsent :", window.nsent)
		log.Debug("rest size:", window.RestSize())
		log.Debug("retry buffer size:", len(s.enqueue.retryBuffer))
		select {
		case <-ctx.Done():
			log.Info("shutdown sender thread")
			reportTick.Stop()
			return

		case <-reportTick.C:
			report()

		case ack := <-s.chAck:
			// debug log
			log.Debug("recv ack : ", &ack)
			log.Debug("transHead :", window.transHead)
			if len(window.window) > 0 {
				log.Debug("window head :", &window.window[0])
			} else {
				log.Debug("no window")
			}
			log.Debug("rto :", window.rtt.RTO)
			log.Debug("rtt :", window.rtt.rtt)
			log.Debug("min rtt :", window.rtt.min)

			if ack.header.Type != TypeACK {
				log.Info("accept not ACK msg :", ack)
				continue
			}

			retry, noack, ok := window.AckSegment(&ack)

			// for statistics
			if ok {
				nsuccess++
			}
			nresend += len(retry)
			nnoack += noack

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
		chRecvFile: make(chan *FileContext, 100),
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
				select {
				case <-ctx.Done():
					return ctx.Err()
				case r.chRecvFile <- f:
				}
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

const (
	mode10  = 0
	modeAll = 1
)

func main() {
	src := flag.String("src", "localhost:19809", "sender address")
	dst := flag.String("dst", "localhost:19810", "receiver address")
	mode := flag.Int("mode", 0, "mode 0: send 100files, 1: send all file")
	path := flag.String("path", filepath.Join("tmp"), "file path")
	loglv := flag.String("log", "point", "log level [error info point debug]")
	cong := flag.String("cong", "vegas", "congestion control algorithm [simple vegas]")
	mtu := flag.Int("mtu", 1500, "MTU size")

	flag.Parse()

	log.SetLevelStr(*loglv)

	if *mode == mode10 {
		go func() {
			log.Error(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	srcaddr, err := net.ResolveUDPAddr("udp4", *src)
	if err != nil {
		log.Panic(err)
	}
	destaddr, err := net.ResolveUDPAddr("udp4", *dst)
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

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rtt := newRTTCollecter(10, &DoubleRTO{})

	var ctrl CongestionControlAlgorithm
	switch *cong {
	case "simple":
		ctrl = NewSimpleControl(10)
	case "vegas":
		ctrl = NewVegasControl(10, rtt, 2, 3.5)
	default:
		log.Error("unknown algorithm :", *cong)
		return
	}
	window := NewWindowManager(ctrl, rtt)

	// sender
	sender := createSender(ctx, &wg, sendConn, *mtu, window)

	receiver := createReceiver(ctx, &wg, recvConn, *mtu)

	if *mode == mode10 {
		log.Info("start debug mode send 10")
		data, err := ioutil.ReadFile(filepath.Join(*path, "sample"))
		if err != nil {
			log.Panic(err)
		}
		const (
			filenum = 10
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
				log.Info("not same data", f.fileno)
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

			// FileContext will not be GCed
			f.data = nil
		}
	} else if *mode == modeAll {
		log.Info("start all file send mode")

		// load all data
		files := make([][]byte, 1000)
		for i := 0; i < 1000; i++ {
			data, err := ioutil.ReadFile(filepath.Join(*path, "src", fmt.Sprintf("%d.bin", i+1)))
			if err != nil {
				log.Panic(err)
			}
			files[i] = data
		}

		// make dest dir
		dstpath := filepath.Join(*path, "dst")
		_ = os.MkdirAll(dstpath, os.ModePerm)

		// send file
		go func() {
			var j int
			for {
				for i := 0; i < 1000; i++ {
					sender.sendFile(ctx, uint32(j), files[i])
					j++
				}
			}
		}()

		// read file
		var idxrecv int
		for {
			f := <-receiver.chRecvFile
			filename := filepath.Join(dstpath, fmt.Sprintf("%d.bin", idxrecv+1))
			if err := ioutil.WriteFile(filename, f.data, os.ModePerm); err != nil {
				log.Panic(err)
			}
			log.Info("file save :", filename)
			idxrecv++

			// FileContext will not be GCed
			f.data = nil
		}
	} else {
		log.Error("invalid mode")
	}

	time.Sleep(100 * time.Millisecond)

	log.Info("shutdown...")

	cancel()
	wg.Wait()
}
