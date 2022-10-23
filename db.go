package ssdb

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

type DB struct {
	mu        sync.Mutex
	ldb       *leveldb.DB
	batch     leveldb.Batch
	binlog    *Binlog
	byteOrder binary.ByteOrder
}

func Open(path string, opts *Options) (*DB, error) {
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &DB{
		ldb:       ldb,
		binlog:    NewBinlog(ldb),
		byteOrder: binary.LittleEndian, // TODO
	}, nil
}

func (db *DB) BeginTxn() *Txn {
	txn := NewTxn(db)
	txn.Begin()
	return txn
}

func (db *DB) WithTxn(f func(txn *Txn) error) error {
	txn := db.BeginTxn()
	defer txn.Rollback()

	if err := f(txn); err != nil {
		return err
	}
	return txn.Commit()
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

func (db *DB) incrby(ctx context.Context, ldbKey []byte, increment int64, eventCmd EventCommand) (val int64, err error) {
	err = db.WithTxn(func(txn *Txn) error {
		val, err = db.incrbyTxn(ctx, txn, ldbKey, increment, StringSet)
		return err
	})

	return val, err
}

func (db *DB) incrbyTxn(ctx context.Context, txn *Txn,
	ldbKey []byte, increment int64, eventCmd EventCommand) (int64, error) {

	oldVal, err := txn.Get(ctx, ldbKey)
	if err != nil {
		return 0, err
	}

	var newVal int64
	if oldVal == nil {
		newVal = increment
	} else {
		// TODO
		newVal, err = strconv.ParseInt(string(oldVal), 10, 64)
		if err != nil {
			return 0, err
		}
		newVal += increment
	}

	txn.Put(ctx, ldbKey, []byte(strconv.FormatInt(newVal, 10)), Sync, eventCmd)

	return newVal, nil
}

func (db *DB) Close() error {
	if db.ldb != nil {
		return db.ldb.Close()
	}
	return nil
}
