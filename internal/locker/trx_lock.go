package locker

import "sync"

type TrxLock struct {
	sync.Mutex
	eventIdx uint64
}

func NewTrxLock() *TrxLock {
	return &TrxLock{
		eventIdx: 1,
	}
}

func (l *TrxLock) EvIdx() uint64 {
	return l.eventIdx
}

func (l *TrxLock) IncrEvIdx() {
	l.eventIdx++
}
