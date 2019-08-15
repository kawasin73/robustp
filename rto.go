package main

import "time"

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
