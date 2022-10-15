package ssdb

import (
	"context"
)

func encodeStringKey(key []byte) []byte {
	buf := make([]byte, 0, 1+len(key))
	buf = append(buf, 'k')
	buf = append(buf, key...)
	return buf
}

func (db *DB) Set(ctx context.Context, key []byte, value []byte) error {
	return db.ldb.Put(encodeStringKey(key), value, nil)
}

func (db *DB) Get(ctx context.Context, key []byte) ([]byte, error) {
	return db.ldb.Get(encodeStringKey(key), nil)
}

func (db *DB) Del(ctx context.Context, key []byte) error {
	return db.ldb.Delete(encodeStringKey(key), nil)
}
