package main

import (
	"math"
	"time"
)

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
	rtt float64
	min float64
	RTO time.Duration

	rttTable []float64
	size     int
	head     int

	rtoCalc RTOCalclater
}

func newRTTCollecter(window int, rtoCalc RTOCalclater) *RTTCollecter {
	rc := &RTTCollecter{
		rttTable: make([]float64, window),
		rtoCalc:  rtoCalc,
		min:      float64(2000 * time.Millisecond),
	}
	rc.RTO = rtoCalc.Update(0, nil)
	return rc
}

func (rc *RTTCollecter) AddRTT(rttd time.Duration) {
	rtt := float64(rttd)
	// queue into rttTable
	if rc.size < len(rc.rttTable) {
		rc.rttTable[rc.size] = rtt
		sum := rc.rtt * float64(rc.size)
		rc.size++
		rc.rtt = (sum + rtt) / float64(rc.size)

		if rtt < rc.min {
			rc.min = rtt
		}
	} else {
		// pop old rtt
		rc.rtt -= rc.rttTable[rc.head] / float64(rc.size)
		if rc.rttTable[rc.head] <= rc.min {
			// refresh min RTT
			rc.min = math.MaxFloat64
			rc.rttTable[rc.head] = rtt
			for _, v := range rc.rttTable {
				if v < rc.min {
					rc.min = v
				}
			}
		}

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
