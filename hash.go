package ssdb

import (
	"context"
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

func encodeHashSizeKey(key []byte) []byte {
	buf := make([]byte, 0, 1+len(key))
	buf = append(buf, 'H')
	buf = append(buf, key...)

	return buf
}

func (db *DB) HGet(ctx context.Context, key []byte, field []byte) ([]byte, error) {
	return db.ldb.Get(encodeHashKey(key, field), nil)
}

func (db *DB) HLen(ctx context.Context, key []byte) (uint64, error) {
	value, err := db.ldb.Get(encodeHashSizeKey(key), nil)
	if err != nil {
		return 0, nil
	}
	return db.byteOrder.Uint64(value), nil
}
