package ssdb

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

var ErrBinlogEventCorrupted = errors.New("ssdb: binlog event corrupted")

type EventType byte

const (
	Noop   EventType = 0
	Sync   EventType = 1
	Mirror EventType = 2
	Copy   EventType = 3
	Ctrl   EventType = 4
)

type EventCommand byte

const (
	None      EventCommand = 0
	StringSet EventCommand = 1
	StringDel EventCommand = 2
	HashSet   EventCommand = 3
	HashDel   EventCommand = 4
	ZSetSet   EventCommand = 5
	ZSetDel   EventCommand = 6

	Begin EventCommand = 7
	End   EventCommand = 8
)

type Event struct {
	Seq  uint64
	Type EventType
	Cmd  EventCommand
	Key  []byte
}

func (e *Event) MarshalBinary() []byte {
	buf := make([]byte, 8, 8+1+1+len(e.Key))
	binary.LittleEndian.PutUint64(buf, e.Seq)
	buf = append(buf, byte(e.Type))
	buf = append(buf, byte(e.Cmd))
	buf = append(buf, e.Key...)
	return buf
}

func (e *Event) UnmarshalBinary(buf []byte) error {
	if len(buf) < 10 {
		return ErrBinlogEventCorrupted
	}
	e.Seq = binary.LittleEndian.Uint64(buf)
	e.Type = EventType(buf[8])
	e.Cmd = EventCommand(buf[9])
	e.Key = buf[10:]

	return nil
}

func encodeEventKey(seq uint64) []byte {
	buf := make([]byte, 1+8)
	buf[0] = 1
	binary.BigEndian.PutUint64(buf[1:], seq)
	return buf
}

type Binlog struct {
	ldb *leveldb.DB
	seq uint64
}

func NewBinlog(ldb *leveldb.DB) *Binlog {
	return &Binlog{
		ldb: ldb,
	}
}

func (b *Binlog) Seq() uint64 {
	return b.seq
}

func (b *Binlog) Get(ctx context.Context, seq uint64) (*Event, error) {
	value, err := b.ldb.Get(encodeEventKey(seq), nil)
	if err != nil {
		return nil, err
	}
	event := &Event{}
	if err := event.UnmarshalBinary(value); err != nil {
		return nil, err
	}
	return event, nil
}
