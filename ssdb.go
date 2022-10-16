package ssdb

import (
	"encoding/binary"
	"errors"

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

func (db *DB) Close() error {
	if db.ldb != nil {
		return db.ldb.Close()
	}
	return nil
}
