package db

import (
	"errors"
	"strconv"
	"time"
)

var NullTime int32

type IDAccount []byte

func (idacc IDAccount) Int() IDAcc {
	var us uint64
	for _, digit := range idacc {
		d := digit - '0'
		if d > 9 {
			break
		}
		us = us*10 + uint64(d)
	}
	return IDAcc(us)
}

type TimeStamp []byte

func (ts TimeStamp) Time() time.Time {
	return time.Unix(int64(ts.Int()), 0).UTC()
}

func (ts TimeStamp) Int() int32 {
	if len(ts) == 0 {
		return NullTime
	}
	var us int64
	mul := int32(1)
	for i, digit := range ts {
		if i == 0 && digit == '-' {
			mul = -1
			continue
		}
		d := digit - '0'
		if d > 9 {
			break
		}
		us = us*10 + int64(d)
	}

	return mul * int32(us)
}

var ErrNotTimeStamp = errors.New("not a timestamp")

func (ts TimeStamp) Validate() error {
	for i, digit := range ts {
		if i == 0 && digit == '-' {
			continue
		}
		d := digit - '0'
		if d > 9 {
			return ErrNotTimeStamp
		}
	}
	return nil
}

func (ts TimeStamp) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatUint(uint64(ts.Int()), 10)), nil
}

func (ts *TimeStamp) UnmarshalJSON(data []byte) error {
	*ts = TimeStamp(data)
	return ts.Validate()
}

type IDAcc uint32

func (id IDAcc) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatUint(uint64(id), 10)), nil
}

func (id *IDAcc) UnmarshalJSON(data []byte) error {
	i, err := strconv.ParseUint(string(data), 10, 32)
	if err != nil {
		return err
	}
	*id = IDAcc(i)
	return nil
}
