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

type FileContext struct {
	fileno  uint32
	data    []byte
	state   []bool
	segSize uint16

	tail       int
	ncompleted int
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
		Seqno:        uint32(f.tail * int(f.segSize)),
		TotalLength:  uint32(len(f.data)),
	}

	i := f.tail
	var (
		partials []PartialAck
		nseg     int
	)
	for f.ncompleted-f.tail-nseg > 0 {
		// search for first completed segment from tail
		for ; !f.state[i]; i++ {
		}
		seqno := uint32(i) * uint32(f.segSize)
		length := uint32(0)
		for ; i < len(f.state) && f.state[i]; i++ {
			length += uint32(f.segSize)
			nseg++
		}
		partials = append(partials, PartialAck{seqno: seqno, length: length})
	}
	EncodePartialAck(buf, &header, partials)
	header.Encode(buf)
	return buf[:header.Length]
}

func (f *FileContext) SaveData(header *Header, buf []byte) error {
	if f.fileno != header.Fileno {
		return fmt.Errorf("invalid fileno for save data")
	}
	stateno := int(header.Seqno) / int(f.segSize)

	if !f.state[stateno] {
		copy(f.data[header.Seqno:header.Seqno+uint32(header.Length-uint16(header.HeaderLength))], buf[header.HeaderLength:])
		f.state[stateno] = true
		f.ncompleted++

		// update tail stateno
		completed := f.tail
		for ; completed < len(f.state) && f.state[completed]; completed++ {
		}
		f.tail = completed
	} else {
		// already ack
	}
	return nil
}

func (f *FileContext) AckData(ack *AckMsg) (int, error) {
	if f.fileno != ack.header.Fileno {
		return 0, fmt.Errorf("invalid fileno for save data")
	}
	stateno := int(ack.header.Seqno) / int(f.segSize)
	var n int

	// update tail stateno
	if f.tail < stateno {
		for i := f.tail; i < stateno; i++ {
			if !f.state[i] {
				f.state[i] = true
				n++
				f.ncompleted++
			}
		}
		f.tail = stateno
	}

	// update partial ack
	for _, partial := range ack.partials {
		head := partial.seqno / uint32(f.segSize)
		l := partial.length / uint32(f.segSize)
		for i := uint32(0); i < l; i++ {
			if !f.state[head+i] {
				f.state[head+i] = true
				n++
				f.ncompleted++
			}
		}
	}

	return n, nil
}

func (f *FileContext) IsCompleted(seqno uint32) bool {
	return f.state[seqno/uint32(f.segSize)]
}

func (f *FileContext) IsAllCompleted() bool {
	return f.tail == len(f.state)
}

type WaitItem struct {
	sentat time.Time
	fctx   *FileContext
	seqno  uint32
}

type RTOCalclater interface {
	Update(rtt float64, rttTable []float64) time.Duration
}

type DoubleRTO struct {
}

func (r *DoubleRTO) Update(rtt float64, rttTable []float64) time.Duration {
	if rtt == 0 {
		return 200 * time.Millisecond
	}
	return time.Duration(rtt * 2)
}

type RTTCollecter struct {
	table map[uint32]map[uint32]time.Time
	rtt   float64
	RTO   time.Duration

	rttTable []float64
	size     int
	head     int

	rtoCalc RTOCalclater
}

func newRTTCollecter(window int, rtoCalc RTOCalclater) *RTTCollecter {
	rc := &RTTCollecter{
		table:    make(map[uint32]map[uint32]time.Time),
		rttTable: make([]float64, window),
		rtoCalc:  rtoCalc,
	}
	rc.RTO = rtoCalc.Update(0, nil)
	return rc
}

func (rc *RTTCollecter) Send(fileno, seqno uint32, t time.Time) {
	file, ok := rc.table[fileno]
	if !ok {
		file = make(map[uint32]time.Time)
		rc.table[fileno] = file
	}
	file[seqno] = t
}

func (rc *RTTCollecter) Recv(fileno, seqno uint32, t time.Time) {
	if file, ok := rc.table[fileno]; !ok {
		return
	} else if st, ok := file[seqno]; !ok {
		return
	} else {
		rtt := t.Sub(st)
		rc.addRTT(float64(rtt))

		// remove from table
		delete(file, seqno)
		if len(file) == 0 {
			delete(rc.table, fileno)
		}
	}
}

func (rc *RTTCollecter) Remove(fileno, seqno uint32) {
	if file, ok := rc.table[fileno]; !ok {
		return
	} else {
		// remove from table
		delete(file, seqno)
		if len(file) == 0 {
			delete(rc.table, fileno)
		}
	}
}

func (rc *RTTCollecter) addRTT(rtt float64) {
	// queue into rttTable
	if rc.size < len(rc.rttTable) {
		rc.rttTable[rc.size] = rtt
		sum := rc.rtt * float64(rc.size)
		rc.size++
		rc.rtt = (sum + rtt) / float64(rc.size)
	} else {
		rc.rtt -= rc.rttTable[rc.head] / float64(rc.size)
		rc.rtt += rtt / float64(rc.size)
		rc.rttTable[rc.head] = rtt
		rc.head++
		if rc.head == len(rc.rttTable) {
			rc.head = 0
		}
	}

	// calc RTO
	rc.RTO = rc.rtoCalc.Update(rc.rtt, rc.rttTable[:rc.size])
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
