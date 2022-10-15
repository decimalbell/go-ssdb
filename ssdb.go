package ssdb

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type DB struct {
	ldb *leveldb.DB
}

func Open(path string, opts *Options) (*DB, error) {
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &DB{
		ldb: ldb,
	}, nil
}

func (db *DB) Close() error {
	if db.ldb != nil {
		return db.ldb.Close()
	}
	return nil
}
