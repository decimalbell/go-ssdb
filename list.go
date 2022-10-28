package ssdb

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

const (
	frontSeq uint64 = 2
	backSeq  uint64 = 3
)

func encodeListKey(key []byte, seq uint64) []byte {
	buf := make([]byte, 1+1+len(key)+8)
	buf[0] = 'q'
	buf[1] = byte(len(key))
	copy(buf[2:], key)
	binary.BigEndian.PutUint64(buf[1+1+len(key):], seq)
	return buf
}

func encodeListLenKey(key []byte) []byte {
	buf := make([]byte, 0, 1+len(key))
	buf = append(buf, 'Q')
	buf = append(buf, key...)
	return buf
}

func (db *DB) LIndex(ctx context.Context, key []byte, index int64) ([]byte, error) {
	var (
		seq int64
		err error
	)
	if index >= 0 {
		seq, err = db.lget(encodeListKey(key, frontSeq))
		if err != nil {
			return nil, err
		}
		seq += index
	} else {
		seq, err = db.lget(encodeListKey(key, backSeq))
		if err != nil {
			return nil, err
		}
		seq += index + 1
	}

	return db.get(encodeListKey(key, uint64(seq)))
}

func (db *DB) LLen(ctx context.Context, key []byte) (int64, error) {
	return db.lget(encodeListLenKey(key))
}

func (db *DB) lget(ldbKey []byte) (int64, error) {
	value, err := db.ldb.Get(ldbKey, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(value) != 8 {
		return 0, nil // TODO
	}
	return int64(db.byteOrder.Uint64(value)), nil
}
