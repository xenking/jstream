package jstream

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"sync/atomic"
	"unicode/utf16"

	"github.com/xenking/jstream/internal"
	"github.com/xenking/jstream/internal/scanner"
	data "github.com/xenking/jstream/internal/scratch"
)

// ValueType - defines the type of each JSON value
type ValueType int

// Different types of JSON value
const (
	Unknown ValueType = iota
	Null
	String
	Number
	Boolean
	Array
	Object
)

// MetaValue wraps a decoded interface value with the document
// position and depth at which the value was parsed
type MetaValue struct {
	Offset    int
	Length    int
	Depth     int
	Keys      []string
	Value     interface{}
	ValueType ValueType
}

// KV contains a key and value pair parsed from a decoded object
type KV struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

// KVS - represents key values in an JSON object
type KVS []KV

// MarshalJSON - implements converting a KVS datastructure into a JSON
// object with multiple keys and values.
func (kvs KVS) MarshalJSON() ([]byte, error) {
	b := new(bytes.Buffer)
	b.Write([]byte("{"))
	for i, kv := range kvs {
		b.Write([]byte("\"" + kv.Key + "\"" + ":"))
		valBuf, err := json.Marshal(kv.Value)
		if err != nil {
			return nil, err
		}
		b.Write(valBuf)
		if i < len(kvs)-1 {
			b.Write([]byte(","))
		}
	}
	b.Write([]byte("}"))
	return b.Bytes(), nil
}

// Decoder wraps an io.Reader to provide incremental decoding of
// JSON values
type Decoder struct {
	*scanner.Scanner
	emitDepth     int
	emitKV        bool
	emitRecursive bool
	objectAsKVS   bool

	depth   int
	scratch *data.Scratch
	metaCh  chan *MetaValue
	err     error

	// follow line position to add context to errors
	lineNo    int
	lineStart int64
}

// NewDecoder creates new Decoder to read JSON values at the provided
// emitDepth from the provider io.Reader.
// If emitDepth is < 0, values at every depth will be emitted.
func NewDecoder(r io.Reader, emitDepth int) *Decoder {
	d := &Decoder{
		Scanner:   scanner.New(r),
		emitDepth: emitDepth,
		scratch:   &data.Scratch{Data: make([]byte, 1024)},
		metaCh:    make(chan *MetaValue, 128),
	}
	if emitDepth < 0 {
		d.emitDepth = 0
		d.emitRecursive = true
	}
	return d
}

// ObjectAsKVS - by default JSON returns map[string]interface{} this
// is usually fine in most cases, but when you need to preserve the
// input order its not a right data structure. To preserve input
// order please use this option.
func (d *Decoder) ObjectAsKVS() *Decoder {
	d.objectAsKVS = true
	return d
}

// EmitKV enables emitting a jstream.KV struct when the items(s) parsed
// at configured emit depth are within a JSON object. By default, only
// the object values are emitted.
func (d *Decoder) EmitKV() *Decoder {
	d.emitKV = true
	return d
}

// Recursive enables emitting all values at a depth higher than the
// configured emit depth; e.g. if an array is found at emit depth, all
// values within the array are emitted to the stream, then the array
// containing those values is emitted.
func (d *Decoder) Recursive() *Decoder {
	d.emitRecursive = true
	return d
}

// Stream begins decoding from the underlying reader and returns a
// streaming MetaValue channel for JSON values at the configured emitDepth.
func (d *Decoder) Stream() chan *MetaValue {
	go d.decode()
	return d.metaCh
}

// Pos returns the number of bytes consumed from the underlying reader
func (d *Decoder) GetPos() int { return int(d.Pos) }

// Err returns the most recent decoder error if any, or nil
func (d *Decoder) Err() error { return d.err }

// Decode parses the JSON-encoded data and returns an interface value
func (d *Decoder) decode() {
	defer close(d.metaCh)
	d.skipSpaces()
	for d.Pos < atomic.LoadInt64(&d.End) {
		_, err := d.emitAny([]string{})
		if err != nil {
			d.err = err
			break
		}
		d.skipSpaces()
	}
}

func (d *Decoder) emitAny(pKeys []string) (interface{}, error) {
	if d.Pos >= atomic.LoadInt64(&d.End) {
		return nil, d.mkError(internal.ErrUnexpectedEOF)
	}
	offset := d.Pos - 1
	i, t, err := d.any(pKeys)
	if d.willEmit() {
		d.metaCh <- &MetaValue{
			Offset:    int(offset),
			Length:    int(d.Pos - offset),
			Depth:     d.depth,
			Keys:      pKeys,
			Value:     i,
			ValueType: t,
		}
	}
	return i, err
}

// return whether, at the current depth, the value being decoded will
// be emitted to stream
func (d *Decoder) willEmit() bool {
	if d.emitRecursive {
		return d.depth >= d.emitDepth
	}
	return d.depth == d.emitDepth
}

// any used to decode any valid JSON value, and returns an
// interface{} that holds the actual data
func (d *Decoder) any(pKeys []string) (interface{}, ValueType, error) {
	c := d.Cur()

	switch c {
	case '"':
		i, err := d.string()
		return i, String, err
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		ii, err := d.number()
		if err != nil {
			return nil, Unknown, err
		}
		switch v := ii.(type) {
		case int64, float64:
			return v, Number, nil
		default:
			return nil, Number, d.mkError(internal.ErrSyntax, "invalid number type")
		}
	case '-':
		if c = d.Next(); c < '0' && c > '9' {
			return nil, Unknown, d.mkError(internal.ErrSyntax, "in negative numeric literal")
		}
		ni, err := d.number()
		if err != nil {
			return nil, Unknown, err
		}
		switch n := ni.(type) {
		case int64:
			return -n, Number, nil
		case float64:
			return -n, Number, nil
		default:
			return nil, Number, d.mkError(internal.ErrSyntax, "invalid number type")
		}
	case 'f':
		if d.Remaining() < 4 {
			return nil, Unknown, d.mkError(internal.ErrUnexpectedEOF)
		}
		if d.Next() == 'a' && d.Next() == 'l' && d.Next() == 's' && d.Next() == 'e' {
			return false, Boolean, nil
		}
		return nil, Unknown, d.mkError(internal.ErrSyntax, "in literal false")
	case 't':
		if d.Remaining() < 3 {
			return nil, Unknown, d.mkError(internal.ErrUnexpectedEOF)
		}
		if d.Next() == 'r' && d.Next() == 'u' && d.Next() == 'e' {
			return true, Boolean, nil
		}
		return nil, Unknown, d.mkError(internal.ErrSyntax, "in literal true")
	case 'n':
		if d.Remaining() < 3 {
			return nil, Unknown, d.mkError(internal.ErrUnexpectedEOF)
		}
		if d.Next() == 'u' && d.Next() == 'l' && d.Next() == 'l' {
			return nil, Null, nil
		}
		return nil, Unknown, d.mkError(internal.ErrSyntax, "in literal null")
	case '[':
		i, err := d.array(pKeys)
		return i, Array, err
	case '{':
		var i interface{}
		var err error
		if d.objectAsKVS {
			i, err = d.objectOrdered(pKeys)
		} else {
			i, err = d.object(pKeys)
		}
		return i, Object, err
	default:
		return nil, Unknown, d.mkError(internal.ErrSyntax, "looking for beginning of value")
	}
}

// string called by `any` or `object`(for map keys) after reading `"`
func (d *Decoder) string() (string, error) {
	d.scratch.Reset()

	var (
		c = d.Next()
	)

scan:
	for {
		switch {
		case c == '"':
			return string(d.scratch.Bytes()), nil
		case c == '\\':
			c = d.Next()
			goto scanEsc
		case c < 0x20:
			return "", d.mkError(internal.ErrSyntax, "in string literal")
		// Coerce to well-formed UTF-8.
		default:
			d.scratch.Add(c)
			if d.Remaining() == 0 {
				return "", d.mkError(internal.ErrSyntax, "in string literal")
			}
			c = d.Next()
		}
	}

scanEsc:
	switch c {
	case '"', '\\', '/', '\'':
		d.scratch.Add(c)
	case 'u':
		goto scanU
	case 'b':
		d.scratch.Add('\b')
	case 'f':
		d.scratch.Add('\f')
	case 'n':
		d.scratch.Add('\n')
	case 'r':
		d.scratch.Add('\r')
	case 't':
		d.scratch.Add('\t')
	default:
		return "", d.mkError(internal.ErrSyntax, "in string escape code")
	}
	c = d.Next()
	goto scan

scanU:
	r := d.u4()
	if r < 0 {
		return "", d.mkError(internal.ErrSyntax, "in unicode escape sequence")
	}

	// check for proceeding surrogate pair
	c = d.Next()
	if !utf16.IsSurrogate(r) || c != '\\' {
		d.scratch.AddRune(r)
		goto scan
	}
	if c = d.Next(); c != 'u' {
		d.scratch.AddRune(r)
		goto scanEsc
	}

	r2 := d.u4()
	if r2 < 0 {
		return "", d.mkError(internal.ErrSyntax, "in unicode escape sequence")
	}

	// write surrogate pair
	d.scratch.AddRune(utf16.DecodeRune(r, r2))
	c = d.Next()
	goto scan
}

// u4 reads four bytes following a \u escape
func (d *Decoder) u4() rune {
	// logic taken from:
	// github.com/buger/jsonparser/blob/master/escape.go#L20
	var h [4]int
	for i := 0; i < 4; i++ {
		c := d.Next()
		switch {
		case c >= '0' && c <= '9':
			h[i] = int(c - '0')
		case c >= 'A' && c <= 'F':
			h[i] = int(c - 'A' + 10)
		case c >= 'a' && c <= 'f':
			h[i] = int(c - 'a' + 10)
		default:
			return -1
		}
	}
	return rune(h[0]<<12 + h[1]<<8 + h[2]<<4 + h[3])
}

// number called by `any` after reading number between 0 to 9
func (d *Decoder) number() (interface{}, error) {
	d.scratch.Reset()

	var (
		c       = d.Cur()
		isFloat bool
	)

	// digits first
	switch {
	case c == '0':
		d.scratch.Add(c)
		c = d.Next()
	case '1' <= c && c <= '9':
		for ; c >= '0' && c <= '9'; c = d.Next() {
			d.scratch.Add(c)
		}
	}

	// . followed by 1 or more digits
	if c == '.' {
		isFloat = true
		d.scratch.Add(c)

		// first char following must be digit
		if c = d.Next(); c < '0' && c > '9' {
			return 0, d.mkError(internal.ErrSyntax, "after decimal point in numeric literal")
		}
		d.scratch.Add(c)

		for {
			if d.Remaining() == 0 {
				return 0, d.mkError(internal.ErrUnexpectedEOF)
			}
			if c = d.Next(); c < '0' || c > '9' {
				break
			}
			d.scratch.Add(c)
		}
	}

	// e or E followed by an optional - or + and
	// 1 or more digits.
	if c == 'e' || c == 'E' {
		isFloat = true
		d.scratch.Add(c)

		if c = d.Next(); c == '+' || c == '-' {
			d.scratch.Add(c)
			if c = d.Next(); c < '0' || c > '9' {
				return 0, d.mkError(internal.ErrSyntax, "in exponent of numeric literal")
			}
			d.scratch.Add(c)
		}
		for ; c >= '0' && c <= '9'; c = d.Next() {
			d.scratch.Add(c)
		}
	}

	d.Back()

	if isFloat {
		var (
			err error
			n   float64
		)
		sn := string(d.scratch.Bytes())
		if n, err = strconv.ParseFloat(sn, 64); err != nil {
			return 0, err
		}
		return n, err
	}

	sn := string(d.scratch.Bytes())
	return strconv.ParseInt(sn, 10, 64)
}

// array accept valid JSON array value
func (d *Decoder) array(pKeys []string) ([]interface{}, error) {
	d.depth++
	parentKeys := append(pKeys, "")
	var (
		c     byte
		v     interface{}
		err   error
		array = make([]interface{}, 0)
	)

	// look ahead for ] - if the array is empty.
	if c = d.skipSpaces(); c == ']' {
		goto out
	}

scan:
	if v, err = d.emitAny(parentKeys); err != nil {
		goto out
	}

	if d.depth > d.emitDepth { // skip alloc for array if it won't be emitted
		array = append(array, v)
	}

	// next token must be ',' or ']'
	switch c = d.skipSpaces(); c {
	case ',':
		d.skipSpaces()
		goto scan
	case ']':
		goto out
	default:
		err = d.mkError(internal.ErrSyntax, "after array element")
	}

out:
	d.depth--
	return array, err
}

// object accept valid JSON array value
func (d *Decoder) object(pKeys []string) (map[string]interface{}, error) {
	d.depth++

	var (
		c   byte
		k   string
		v   interface{}
		t   ValueType
		err error
		obj map[string]interface{}
	)

	// skip allocating map if it will not be emitted
	if d.depth > d.emitDepth {
		obj = make(map[string]interface{})
	}

	// if the object has no keys
	if c = d.skipSpaces(); c == '}' {
		goto out
	}

scan:
	for {
		offset := d.Pos - 1

		// read string key
		if c != '"' {
			err = d.mkError(internal.ErrSyntax, "looking for beginning of object key string")
			break
		}
		if k, err = d.string(); err != nil {
			break
		}

		// read colon before value
		if c = d.skipSpaces(); c != ':' {
			err = d.mkError(internal.ErrSyntax, "after object key")
			break
		}

		// read value
		d.skipSpaces()
		keys := append(pKeys, k)
		if d.emitKV {
			if v, t, err = d.any(keys); err != nil {
				break
			}
			if d.willEmit() {
				d.metaCh <- &MetaValue{
					Offset:    int(offset),
					Length:    int(d.Pos - offset),
					Depth:     d.depth,
					Keys:      keys,
					Value:     KV{k, v},
					ValueType: t,
				}
			}
		} else {
			if v, err = d.emitAny(keys); err != nil {
				break
			}
		}

		if obj != nil {
			obj[k] = v
		}

		// next token must be ',' or '}'
		switch c = d.skipSpaces(); c {
		case '}':
			goto out
		case ',':
			c = d.skipSpaces()
			goto scan
		default:
			err = d.mkError(internal.ErrSyntax, "after object key:value pair")
			goto out
		}
	}

out:
	d.depth--
	return obj, err
}

// object (ordered) accept valid JSON array value
func (d *Decoder) objectOrdered(pKeys []string) (KVS, error) {
	d.depth++

	var (
		c   byte
		k   string
		v   interface{}
		t   ValueType
		err error
		obj KVS
	)

	// skip allocating map if it will not be emitted
	if d.depth > d.emitDepth {
		obj = make(KVS, 0)
	}

	// if the object has no keys
	if c = d.skipSpaces(); c == '}' {
		goto out
	}

scan:
	for {
		offset := d.Pos - 1

		// read string key
		if c != '"' {
			err = d.mkError(internal.ErrSyntax, "looking for beginning of object key string")
			break
		}
		if k, err = d.string(); err != nil {
			break
		}

		// read colon before value
		if c = d.skipSpaces(); c != ':' {
			err = d.mkError(internal.ErrSyntax, "after object key")
			break
		}

		// read value
		d.skipSpaces()
		keys := append(pKeys, k)
		if d.emitKV {
			if v, t, err = d.any(keys); err != nil {
				break
			}
			if d.willEmit() {
				d.metaCh <- &MetaValue{
					Offset:    int(offset),
					Length:    int(d.Pos - offset),
					Depth:     d.depth,
					Keys:      keys,
					Value:     KV{k, v},
					ValueType: t,
				}
			}
		} else {
			if v, err = d.emitAny(keys); err != nil {
				break
			}
		}

		if obj != nil {
			obj = append(obj, KV{k, v})
		}

		// next token must be ',' or '}'
		switch c = d.skipSpaces(); c {
		case '}':
			goto out
		case ',':
			c = d.skipSpaces()
			goto scan
		default:
			err = d.mkError(internal.ErrSyntax, "after object key:value pair")
			goto out
		}
	}

out:
	d.depth--
	return obj, err
}

// returns the next char after white spaces
func (d *Decoder) skipSpaces() byte {
	for d.Pos < atomic.LoadInt64(&d.End) {
		switch c := d.Next(); c {
		case '\n':
			d.lineStart = d.Pos
			d.lineNo++
			continue
		case ' ', '\t', '\r':
			continue
		default:
			return c
		}
	}
	return 0
}

// create syntax errors at current position, with optional context
func (d *Decoder) mkError(err internal.SyntaxError, context ...string) error {
	if len(context) > 0 {
		err.Context = context[0]
	}
	err.AtChar = d.Cur()
	err.Pos[0] = d.lineNo + 1
	err.Pos[1] = int(d.Pos - d.lineStart)
	return err
}
