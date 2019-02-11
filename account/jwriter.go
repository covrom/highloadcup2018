package account

import (
	"io"
	"strconv"
	"unicode/utf8"
)

// Flags describe various encoding options. The behavior may be actually implemented in the encoder, but
// Flags field in Writer is used to set and pass them around.
type Flags int

const (
	NilMapAsEmpty   Flags = 1 << iota // Encode nil map as '{}' rather than 'null'.
	NilSliceAsEmpty                   // Encode nil slice as '[]' rather than 'null'.
)

// Writer is a JSON writer.
type Writer struct {
	Flags Flags

	Error        error
	Buffer       io.Writer
	NoEscapeHTML bool
}

// RawByte appends raw binary data to the buffer.
func (w *Writer) RawByte(c byte) {
	w.Buffer.Write([]byte{c})
}

// RawByte appends raw binary data to the buffer.
func (w *Writer) RawString(s string) {
	io.WriteString(w.Buffer, s)
}

// Raw appends raw binary data to the buffer or sets the error if it is given. Useful for
// calling with results of MarshalJSON-like functions.
func (w *Writer) Raw(data []byte, err error) {
	switch {
	case w.Error != nil:
		return
	case err != nil:
		w.Error = err
	case len(data) > 0:
		w.Buffer.Write(data)
	default:
		w.RawString("null")
	}
}

// RawText encloses raw binary data in quotes and appends in to the buffer.
// Useful for calling with results of MarshalText-like functions.
func (w *Writer) RawText(data []byte, err error) {
	switch {
	case w.Error != nil:
		return
	case err != nil:
		w.Error = err
	case len(data) > 0:
		w.String(string(data))
	default:
		w.RawString("null")
	}
}

// Base64Bytes appends data to the buffer after base64 encoding it
func (w *Writer) Base64Bytes(data []byte) {
	if data == nil {
		w.RawString("null")
		return
	}
	w.RawByte('"')
	w.base64(data)
	w.RawByte('"')
}

func (w *Writer) Uint64(n uint64) {
	w.RawString(strconv.FormatUint(n, 10))
}

func (w *Writer) Int64(n int64) {
	w.RawString(strconv.FormatInt(n, 10))
}

func (w *Writer) Uint32(n uint32) {
	w.RawString(strconv.FormatUint(uint64(n), 10))
}

func (w *Writer) Int32(n int32) {
	w.RawString(strconv.FormatInt(int64(n), 10))
}

func (w *Writer) Uint64Str(n uint64) {
	w.RawByte('"')
	w.RawString(strconv.FormatUint(n, 10))
	w.RawByte('"')
}

func (w *Writer) Int64Str(n int64) {
	w.RawByte('"')
	w.RawString(strconv.FormatInt(n, 10))
	w.RawByte('"')
}

func (w *Writer) Float32(n float32) {
	w.RawString(strconv.FormatFloat(float64(n), 'g', -1, 32))
}

func (w *Writer) Float32Str(n float32) {
	w.RawByte('"')
	w.RawString(strconv.FormatFloat(float64(n), 'g', -1, 32))
	w.RawByte('"')
}

func (w *Writer) Float64(n float64) {
	w.RawString(strconv.FormatFloat(n, 'g', -1, 64))
}

func (w *Writer) Float64Str(n float64) {
	w.RawByte('"')
	w.RawString(strconv.FormatFloat(n, 'g', -1, 64))
	w.RawByte('"')
}

func (w *Writer) Bool(v bool) {
	if v {
		w.RawString("true")
	} else {
		w.RawString("false")
	}
}

const chars = "0123456789abcdef"

func isNotEscapedSingleChar(c byte, escapeHTML bool) bool {
	// Note: might make sense to use a table if there are more chars to escape. With 4 chars
	// it benchmarks the same.
	if escapeHTML {
		return c != '<' && c != '>' && c != '&' && c != '\\' && c != '"' && c >= 0x20 && c < utf8.RuneSelf
	} else {
		return c != '\\' && c != '"' && c >= 0x20 && c < utf8.RuneSelf
	}
}

func (w *Writer) String(s string) {
	w.RawByte('"')

	// Portions of the string that contain no escapes are appended as
	// byte slices.

	p := 0 // last non-escape symbol

	for i := 0; i < len(s); {
		c := s[i]

		if isNotEscapedSingleChar(c, !w.NoEscapeHTML) {
			// single-width character, no escaping is required
			i++
			continue
		} else if c < utf8.RuneSelf {
			// single-with character, need to escape
			w.RawString(s[p:i])
			switch c {
			case '\t':
				w.RawString(`\t`)
			case '\r':
				w.RawString(`\r`)
			case '\n':
				w.RawString(`\n`)
			case '\\':
				w.RawString(`\\`)
			case '"':
				w.RawString(`\"`)
			default:
				w.RawString(`\u00`)
				w.RawByte(chars[c>>4])
				w.RawByte(chars[c&0xf])
			}

			i++
			p = i
			continue
		}

		// broken utf
		runeValue, runeWidth := utf8.DecodeRuneInString(s[i:])
		if runeValue == utf8.RuneError && runeWidth == 1 {
			w.RawString(s[p:i])
			w.RawString(`\ufffd`)
			i++
			p = i
			continue
		}

		// jsonp stuff - tab separator and line separator
		if runeValue == '\u2028' || runeValue == '\u2029' {
			w.RawString(s[p:i])
			w.RawString(`\u202`)
			w.RawByte(chars[runeValue&0xf])
			i += runeWidth
			p = i
			continue
		}
		i += runeWidth
	}
	w.RawString(s[p:])
	w.RawByte('"')
}

const encode = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
const padChar = '='

func (w *Writer) base64(in []byte) {

	if len(in) == 0 {
		return
	}

	si := 0
	n := (len(in) / 3) * 3

	for si < n {
		// Convert 3x 8bit source bytes into 4 bytes
		val := uint(in[si+0])<<16 | uint(in[si+1])<<8 | uint(in[si+2])
		w.RawByte(encode[val>>18&0x3F])
		w.RawByte(encode[val>>12&0x3F])
		w.RawByte(encode[val>>6&0x3F])
		w.RawByte(encode[val&0x3F])
		si += 3
	}

	remain := len(in) - si
	if remain == 0 {
		return
	}

	// Add the remaining small block
	val := uint(in[si+0]) << 16
	if remain == 2 {
		val |= uint(in[si+1]) << 8
	}
	w.RawByte(encode[val>>18&0x3F])
	w.RawByte(encode[val>>12&0x3F])

	switch remain {
	case 2:
		w.RawByte(encode[val>>6&0x3F])
		w.RawByte(byte(padChar))
	case 1:
		w.RawByte(byte(padChar))
		w.RawByte(byte(padChar))
	}
}
