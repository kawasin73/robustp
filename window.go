package main

import (
	"fmt"
	log "github.com/kawasin73/robustp/logger"
	"time"
)

type TransSegment struct {
	sendat  time.Time
	segment *FileSegment
	ack     bool
}

func (s *TransSegment) String() string {
	return fmt.Sprintf("{sendat: %v, segment: %v, ack: %v}", s.sendat.Format(time.RFC3339Nano), s.segment, s.ack)
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

func (w *WindowManager) AckSegment(ack *AckMsg) ([]*FileSegment, int, bool) {
	acktime := time.Now()
	var (
		segs  []*FileSegment
		noack int
		ok    bool
	)

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

	for w.transHead < ack.header.TransId {
		item := w.window[0]
		w.window = w.window[1:]
		w.transHead++

		if !item.ack {
			w.nsent--
			if !item.segment.IsCompleted() {
				log.Pointf("early detect loss segment : %v", &item)
				w.ctrl.Add(CONG_LOSS|CONG_EARLY, item.sendat, 0)
				// resend
				segs = append(segs, item.segment)
			} else {
				noack++
				w.ctrl.Add(CONG_SUCCESS|CONG_NOACK, item.sendat, acktime.Sub(item.sendat))
			}
		}
	}

	// pop from window (vacuum)
	for len(w.window) > 0 && w.window[0].ack {
		w.window = w.window[1:]
		w.transHead++
	}

	return segs, noack, ok
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
				log.Pointf("timeout segment : %v", &item)
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
