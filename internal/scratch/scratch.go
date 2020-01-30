package scratch

import (
	"unicode/utf8"
)

type Scratch struct {
	Data []byte
	fill int
}

// reset scratch buffer
func (s *Scratch) Reset() { s.fill = 0 }

// bytes returns the written contents of scratch buffer
func (s *Scratch) Bytes() []byte { return s.Data[0:s.fill] }

// grow scratch buffer
func (s *Scratch) grow() {
	ndata := make([]byte, cap(s.Data)*2)
	copy(ndata, s.Data[:])
	s.Data = ndata
}

// append single byte to scratch buffer
func (s *Scratch) Add(c byte) {
	if s.fill+1 >= cap(s.Data) {
		s.grow()
	}

	s.Data[s.fill] = c
	s.fill++
}

// append encoded rune to scratch buffer
func (s *Scratch) AddRune(r rune) int {
	if s.fill+utf8.UTFMax >= cap(s.Data) {
		s.grow()
	}

	n := utf8.EncodeRune(s.Data[s.fill:], r)
	s.fill += n
	return n
}
