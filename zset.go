package ssdb

import (
	"bytes"
	"context"
	"errors"
	"strconv"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func encodeZSetMemberKey(key []byte, member []byte) []byte {
	buf := make([]byte, 0, 1+1+len(key)+1+len(member))
	buf = append(buf, 's')
	buf = append(buf, byte(len(key)))
	buf = append(buf, key...)
	buf = append(buf, byte(len(member)))
	buf = append(buf, member...)
	return buf
}

func encodeZSetMemberPrefixKey(key []byte) []byte {
	buf := make([]byte, 0, 1+1+len(key))
	buf = append(buf, 's')
	buf = append(buf, byte(len(key)))
	buf = append(buf, key...)
	return buf
}

func encodeZSetLenKey(key []byte) []byte {
	buf := make([]byte, 0, 1+len(key))
	buf = append(buf, 'Z')
	buf = append(buf, key...)
	return buf
}

func (db *DB) ZRank(ctx context.Context, key []byte, member []byte) (uint64, error) {
	var rank uint64
	prefix := encodeZSetMemberPrefixKey(key)
	target := encodeZSetMemberKey(key, member)
	iter := db.ldb.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		if bytes.Equal(iter.Key(), target) {
			break
		}
		rank++
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return 0, err
	}
	return rank, nil
}

func (db *DB) ZScore(ctx context.Context, key []byte, member []byte) (float64, error) {
	value, err := db.get(encodeZSetMemberKey(key, member))
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(value), 64)
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
