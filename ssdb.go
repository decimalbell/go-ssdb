package ssdb

import (
	"encoding/binary"
	"errors"
	"strconv"

	"github.com/syndtr/goleveldb/leveldb"
)

type DB struct {
	ldb       *leveldb.DB
	byteOrder binary.ByteOrder
}

func Open(path string, opts *Options) (*DB, error) {
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &DB{
		ldb:       ldb,
		byteOrder: binary.LittleEndian, // TODO
	}, nil
}

func (db *DB) exists(ldbKey []byte) (bool, error) {
	_, err := db.ldb.Get(ldbKey, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (db *DB) get(ldbKey []byte) ([]byte, error) {
	value, err := db.ldb.Get(ldbKey, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (db *DB) incrby(ldbKey []byte, increment int64) (int64, error) {
	val, err := db.get(ldbKey)
	if err != nil {
		return 0, err
	}

	var newVal int64
	if val == nil {
		newVal = increment
	} else {
		newVal, err = strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return 0, err
		}
		newVal += increment
	}

	err = db.ldb.Put(ldbKey, []byte(strconv.FormatInt(newVal, 10)), nil)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

func (db *DB) Close() error {
	if db.ldb != nil {
		return db.ldb.Close()
	}
	return nil
}
