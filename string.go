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
	return db.get(encodeStringKey(key))
}

func (db *DB) Del(ctx context.Context, key []byte) error {
	return db.ldb.Delete(encodeStringKey(key), nil)
}

func (db *DB) Incrby(ctx context.Context, key []byte, increment int64) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.incrbyLocked(encodeStringKey(key), increment)
}

func (db *DB) Incr(ctx context.Context, key []byte) (int64, error) {
	return db.Incrby(ctx, key, 1)
}
