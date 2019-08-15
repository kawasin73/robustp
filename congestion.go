package main

import "time"

const (
	CONG_SUCCESS = 1 << 0
	CONG_LOSS    = 1 << 1
	CONG_TIMEOUT = 1 << 2
	CONG_EARLY   = 1 << 3
	CONG_NOACK   = 1 << 4
)

type CongestionControlAlgorithm interface {
	Add(status uint8, sendAt time.Time, rtt time.Duration)
	WindowSize() int
}

type SimpleControl struct {
	size int
}

func NewSimpleControl(initial int) CongestionControlAlgorithm {
	return &SimpleControl{size: initial}
}

func (c *SimpleControl) Add(status uint8, sendAt time.Time, rtt time.Duration) {
	if status&CONG_SUCCESS != 0 {
		c.size++
	} else if c.size > 1 {
		c.size--
	}
}

func (c *SimpleControl) WindowSize() int {
	return c.size
}
