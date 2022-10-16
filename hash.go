package ssdb

import (
	"context"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

func encodeHashKey(key []byte, field []byte) []byte {
	buf := make([]byte, 0, 1+1+len(key)+1+len(field))
	buf = append(buf, 'h')
	buf = append(buf, byte(len(key)))
	buf = append(buf, key...)
	buf = append(buf, '=')
	buf = append(buf, field...)
	return buf
}

func encodeHashLenKey(key []byte) []byte {
	buf := make([]byte, 0, 1+len(key))
	buf = append(buf, 'H')
	buf = append(buf, key...)

	return buf
}

func (db *DB) HSet(ctx context.Context, key []byte, field []byte, value []byte) error {
	ldbKey := encodeHashKey(key, field)
	exists, err := db.exists(ldbKey)
	if err != nil {
		return err
	}

	if err := db.ldb.Put(ldbKey, value, nil); err != nil {
		return err
	}

	if !exists {
		return db.incrHLen(encodeHashLenKey(key))
	}

	return nil
}

func (db *DB) incrHLen(ldbKey []byte) error {
	len, err := db.hlen(ldbKey)
	if err != nil {
		return err
	}
	value := make([]byte, 8)
	db.byteOrder.PutUint64(value, uint64(len+1))
	return db.ldb.Put(ldbKey, value, nil)
}

func (db *DB) HGet(ctx context.Context, key []byte, field []byte) ([]byte, error) {
	return db.ldb.Get(encodeHashKey(key, field), nil)
}

func (db *DB) HLen(ctx context.Context, key []byte) (int64, error) {
	return db.hlen(encodeHashLenKey(key))
}

func (db *DB) hlen(ldbKey []byte) (int64, error) {
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
