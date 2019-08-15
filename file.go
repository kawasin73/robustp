package main

import (
	"fmt"
)

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

func (f *FileContext) DataMsg(buf []byte, offset uint32, transId uint32) []byte {
	header := Header{
		Type:         TypeDATA,
		HeaderLength: RobustPHeaderLen,
		Length:       0,
		Fileno:       f.fileno,
		Offset:       offset,
		TotalLength:  uint32(len(f.data)),
		TransId:      transId,
	}
	size := f.segSize
	if int(offset)+int(size) > len(f.data) {
		size = uint16(len(f.data) - int(offset))
	}
	header.Length = uint16(header.HeaderLength) + size

	header.Encode(buf)
	copy(buf[header.HeaderLength:header.Length], f.data[header.Offset:])
	return buf[:header.Length]
}

func (f *FileContext) AckMsg(buf []byte, transId uint32) []byte {
	header := Header{
		Type:         TypeACK,
		HeaderLength: RobustPHeaderLen,
		Length:       RobustPHeaderLen,
		Fileno:       f.fileno,
		Offset:       uint32(f.tail * int(f.segSize)),
		TotalLength:  uint32(len(f.data)),
		TransId:      transId,
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
		offset := uint32(i) * uint32(f.segSize)
		length := uint32(0)
		for ; i < len(f.state) && f.state[i]; i++ {
			length += uint32(f.segSize)
			nseg++
		}
		partials = append(partials, PartialAck{offset: offset, length: length})
	}
	EncodePartialAck(buf, &header, partials)
	header.Encode(buf)
	return buf[:header.Length]
}

func (f *FileContext) SaveData(header *Header, buf []byte) error {
	if f.fileno != header.Fileno {
		return fmt.Errorf("invalid fileno for save data")
	}
	stateno := int(header.Offset) / int(f.segSize)

	if !f.state[stateno] {
		copy(f.data[header.Offset:header.Offset+uint32(header.Length-uint16(header.HeaderLength))], buf[header.HeaderLength:])
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

func (f *FileContext) AckData(ack *AckMsg) {
	if f.fileno != ack.header.Fileno {
		panic(fmt.Errorf("invalid fileno for save data"))
	}
	stateno := int(ack.header.Offset) / int(f.segSize)

	// update tail stateno
	if f.tail < stateno {
		for i := f.tail; i < stateno; i++ {
			if !f.state[i] {
				f.state[i] = true
				f.ncompleted++
			}
		}
		f.tail = stateno
	}

	// update partial ack
	for _, partial := range ack.partials {
		head := partial.offset / uint32(f.segSize)
		l := partial.length / uint32(f.segSize)
		for i := uint32(0); i < l; i++ {
			if !f.state[head+i] {
				f.state[head+i] = true
				f.ncompleted++
			}
		}
	}
}

func (f *FileContext) IsCompleted(offset uint32) bool {
	return f.state[offset/uint32(f.segSize)]
}

func (f *FileContext) IsAllCompleted() bool {
	return f.tail == len(f.state)
}

type FileSegment struct {
	file   *FileContext
	offset uint32
}

func (fs *FileSegment) PackMsg(buf []byte, transId uint32) ([]byte) {
	return fs.file.DataMsg(buf, fs.offset, transId)
}

func (fs *FileSegment) Ack(msg *AckMsg) {
	fs.file.AckData(msg)
}

func (fs *FileSegment) IsCompleted() bool {
	return fs.file.IsCompleted(fs.offset)
}
