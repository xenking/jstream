package scanner

import (
	"io"
	"sync/atomic"
)

const (
	chunk   = 4095 // ~4k
	maxUint = ^uint(0)
	maxInt  = int64(maxUint >> 1)
)

type Scanner struct {
	Pos       int64 // position in reader
	End       int64
	ipos      int64           // internal buffer position
	ifill     int64           // internal buffer fill
	buf       [chunk + 1]byte // internal buffer (with a lookback size of 1)
	nbuf      [chunk]byte     // next internal buffer
	fillReq   chan struct{}
	fillReady chan int64
}

func New(r io.Reader) *Scanner {
	sr := &Scanner{
		End:       maxInt,
		fillReq:   make(chan struct{}),
		fillReady: make(chan int64),
	}

	go func() {
		var rpos int64 // total bytes read into buffer

		for range sr.fillReq {
		scan:
			n, err := r.Read(sr.nbuf[:])

			if n == 0 {
				switch err {
				case io.EOF: // reader is exhausted
					atomic.StoreInt64(&sr.End, rpos)
					close(sr.fillReady)
					return
				case nil: // no data and no error, retry fill
					goto scan
				default:
					panic(err)
				}
			}

			rpos += int64(n)
			sr.fillReady <- int64(n)
		}
	}()

	sr.fillReq <- struct{}{} // initial fill

	return sr
}

// remaining returns the number of unread bytes
// if EOF for the underlying reader has not yet been found,
// maximum possible integer value will be returned
func (s *Scanner) Remaining() int64 {
	if atomic.LoadInt64(&s.End) == maxInt {
		return maxInt
	}
	return atomic.LoadInt64(&s.End) - s.Pos
}

// read byte at current position (without advancing)
func (s *Scanner) Cur() byte { return s.buf[s.ipos] }

// read next byte
func (s *Scanner) Next() byte {
	if s.Pos >= atomic.LoadInt64(&s.End) {
		return byte(0)
	}
	s.ipos++

	if s.ipos > s.ifill { // internal buffer is exhausted
		s.ifill = <-s.fillReady
		s.buf[0] = s.buf[len(s.buf)-1] // copy current last item to guarantee lookback
		copy(s.buf[1:], s.nbuf[:])     // copy contents of pre-filled next buffer
		s.ipos = 1                     // move to beginning of internal buffer

		// request next fill to be prepared
		if s.End == maxInt {
			s.fillReq <- struct{}{}
		}
	}

	s.Pos++
	return s.buf[s.ipos]
}

// back undoes a previous call to next(), moving backward one byte in the internal buffer.
// as we only guarantee a lookback buffer size of one, any subsequent calls to back()
// before calling next() may panic
func (s *Scanner) Back() {
	if s.ipos <= 0 {
		panic("back buffer exhausted")
	}
	s.ipos--
	s.Pos--
}
