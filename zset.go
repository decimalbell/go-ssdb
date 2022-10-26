package ssdb

import (
	"context"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

func encodeZSetLenKey(key []byte) []byte {
	buf := make([]byte, 0, 1+len(key))
	buf = append(buf, 'Z')
	buf = append(buf, key...)

	return buf
}

func (db *DB) ZCard(ctx context.Context, key []byte) (int64, error) {
	return db.zlen(encodeZSetLenKey(key))
}

func (db *DB) zlen(ldbKey []byte) (int64, error) {
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
