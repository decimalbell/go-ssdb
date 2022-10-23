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
	err := db.WithTxn(func(txn *Txn) error {
		txn.Put(ctx, encodeStringKey(key), value, Copy, StringSet)

		return nil
	})

	return err
}

func (db *DB) Get(ctx context.Context, key []byte) ([]byte, error) {
	return db.get(encodeStringKey(key))
}

func (db *DB) Del(ctx context.Context, key []byte) error {
	err := db.WithTxn(func(txn *Txn) error {
		txn.Delete(ctx, encodeStringKey(key), Copy, StringDel)

		return nil
	})

	return err
}

func (db *DB) Incrby(ctx context.Context, key []byte, increment int64) (int64, error) {
	return db.incrby(ctx, encodeStringKey(key), increment, StringSet)
}

func (db *DB) Incr(ctx context.Context, key []byte) (int64, error) {
	return db.Incrby(ctx, key, 1)
}
