package ssdb

import (
	"context"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
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

func encodeHashKeyPrefix(key []byte) []byte {
	buf := make([]byte, 0, 1+1+len(key)+1)
	buf = append(buf, 'h')
	buf = append(buf, byte(len(key)))
	buf = append(buf, key...)
	buf = append(buf, '=')
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
		return db.incrbyHLen(encodeHashLenKey(key), 1)
	}

	return nil
}

func (db *DB) incrbyHLen(ldbKey []byte, increment int64) error {
	len, err := db.hlen(ldbKey)
	if err != nil {
		return err
	}
	value := make([]byte, 8)
	db.byteOrder.PutUint64(value, uint64(len+increment))
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

func (db *DB) HDel(ctx context.Context, key []byte, field []byte) (bool, error) {
	ldbKey := encodeHashKey(key, field)
	exists, err := db.exists(ldbKey)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	if err := db.ldb.Delete(ldbKey, nil); err != nil {
		return false, err
	}

	err = db.incrbyHLen(encodeHashLenKey(key), -1)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (db *DB) HKeys(ctx context.Context, key []byte) ([][]byte, error) {
	prefix := encodeHashKeyPrefix(key)
	iter := db.ldb.NewIterator(util.BytesPrefix(prefix), nil)
	fields := make([][]byte, 0, 32)
	for iter.Next() {
		field := make([]byte, len(iter.Key())-len(prefix))
		copy(field, iter.Key()[len(prefix):])
		fields = append(fields, field)
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return fields, nil
}

func (db *DB) HVals(ctx context.Context, key []byte) ([][]byte, error) {
	prefix := encodeHashKeyPrefix(key)
	iter := db.ldb.NewIterator(util.BytesPrefix(prefix), nil)
	values := make([][]byte, 0, 32)
	for iter.Next() {
		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())
		values = append(values, value)
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return values, nil
}

func (db *DB) HGetAll(ctx context.Context, key []byte) ([][]byte, error) {
	prefix := encodeHashKeyPrefix(key)
	iter := db.ldb.NewIterator(util.BytesPrefix(prefix), nil)
	all := make([][]byte, 0, 64)
	for iter.Next() {
		field := make([]byte, len(iter.Key())-len(prefix))
		copy(field, iter.Key()[len(prefix):])

		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())

		all = append(all, field, value)
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return all, nil
}

func (db *DB) HStrLen(ctx context.Context, key []byte, field []byte) (int, error) {
	value, err := db.get(encodeHashKey(key, field))
	if err != nil {
		return 0, err
	}
	return len(value), nil
}
