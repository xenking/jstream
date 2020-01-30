package internal

import (
	"fmt"
	"strconv"
)

// Predefined errors
var (
	ErrSyntax        = SyntaxError{msg: "invalid character"}
	ErrUnexpectedEOF = SyntaxError{msg: "unexpected end of JSON input"}
)

type errPos [2]int // line number, byte offset where error occurred

type SyntaxError struct {
	msg     string // description of error
	Context string // additional error context
	Pos     errPos
	AtChar  byte
}

func (e SyntaxError) Error() string {
	loc := fmt.Sprintf("%s [%d,%d]", quoteChar(e.AtChar), e.Pos[0], e.Pos[1])
	return fmt.Sprintf("%s %s: %s", e.msg, e.Context, loc)
}

// quoteChar formats c as a quoted character literal
func quoteChar(c byte) string {
	// special cases - different from quoted strings
	if c == '\'' {
		return `'\''`
	}
	if c == '"' {
		return `'"'`
	}

	// use quoted string with different quotation marks
	s := strconv.Quote(string(c))
	return "'" + s[1:len(s)-1] + "'"
}
