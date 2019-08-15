package main

import "fmt"

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
