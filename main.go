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

type WaitItem struct {
	sentat time.Time
	fctx   *FileContext
	seqno  uint32
}

type Sender struct {
	segSize    uint16
	chSendFile chan *FileContext
	rtt        *RTTCollecter
}

func createSender(conn *net.UDPConn, mtu int, rtt *RTTCollecter) *Sender {
	mss := uint16(mtu - IPHeaderLen - UDPHeaderLen)
	segSize := uint16(mss - RobustPHeaderLen)
	s := &Sender{
		segSize:    segSize,
		chSendFile: make(chan *FileContext),
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
	var timers []*WaitItem
	buf := make([]byte, RobustPHeaderLen+s.segSize)

	files := make(map[uint32]*FileContext)
	timer := time.NewTimer(0)
	timer.Stop()

	var (
		chQueueFile <-chan *FileContext = s.chSendFile
		queueFile   *FileContext
		queueSeqno  int
		window      = 260
		nsent       int
	)

	queueFirst := func() {
		if queueFile == nil {
			return
		}
		restartTimer := len(timers) == 0
		for ; queueSeqno < len(queueFile.data) && nsent < window; queueSeqno += int(queueFile.segSize) {
			sendbuf := queueFile.DataMsg(buf, uint32(queueSeqno))
			_, err := conn.Write(sendbuf)
			if err != nil {
				log.Panic(err)
				return
			}
			now := time.Now()
			timers = append(timers, &WaitItem{
				sentat: now,
				fctx:   queueFile,
				seqno:  uint32(queueSeqno),
			})
			s.rtt.Send(queueFile.fileno, uint32(queueSeqno), now)
			nsent++
		}
		if queueSeqno >= len(queueFile.data) {
			// finish to queue all msg in file
			chQueueFile = s.chSendFile
			queueFile = nil
			queueSeqno = 0
		}
		if restartTimer && len(timers) > 0 {
			// reset timer
			timer.Reset(timers[0].sentat.Add(s.rtt.RTO).Sub(time.Now()))
		}
	}

	for {
		select {
		case ack := <-chAck:
			log.Debug("recv ack : ", ack)
			f, ok := files[ack.header.Fileno]
			if !ok {
				log.Infof("recv ack : fileno %d is not found\n", ack.header.Fileno)
				continue
			}
			if nack, err := f.AckData(&ack); err != nil {
				log.Panic(fmt.Errorf("recv ack : %v", err))
			} else {
				nsent -= nack
			}
			// TODO: get time at exactly recv
			s.rtt.Recv(ack.header.Fileno, ack.header.Seqno, time.Now())
			if f.IsAllCompleted() {
				log.Infof("send finished fileno %d\n", f.fileno)
				delete(files, f.fileno)
			}
			queueFirst()

		case now := <-timer.C:
			for len(timers) > 0 && timers[0].sentat.Add(s.rtt.RTO).Before(now) {
				// pop item
				item := timers[0]
				timers = timers[1:]

				s.rtt.Remove(item.fctx.fileno, item.seqno)

				if !item.fctx.IsCompleted(item.seqno) {
					log.Infof("timeout : fileno: %d, seqno: %d", item.fctx.fileno, item.seqno)
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
				timer.Reset(timers[0].sentat.Add(s.rtt.RTO).Sub(time.Now()))
			}

		case f := <-chQueueFile:
			files[f.fileno] = f
			chQueueFile = nil
			queueFile = f
			queueFirst()
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
					// TODO: remove file context
					// それ以降に Data が送られて来ないように ACK + FIN によるコネクションを閉じる考え方が必要。
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
