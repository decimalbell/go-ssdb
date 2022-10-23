package ssdb

import (
	"context"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

type Txn struct {
	db     *DB
	mu     *sync.Mutex
	ldb    *leveldb.DB
	batch  *leveldb.Batch
	binlog *Binlog
	seq    uint64
}

func NewTxn(db *DB) *Txn {
	txn := &Txn{
		db:     db,
		mu:     &db.mu,
		ldb:    db.ldb,
		batch:  &db.batch,
		binlog: db.binlog,
	}
	return txn
}

func (txn *Txn) Begin() {
	txn.mu.Lock()
	txn.seq = txn.binlog.seq
}

func (txn *Txn) Get(ctx context.Context, ldbKey []byte) ([]byte, error) {
	return txn.db.get(ldbKey)
}

func (txn *Txn) Put(ctx context.Context, ldbKey []byte, value []byte) {
	txn.batch.Put(ldbKey, value)
}

func (txn *Txn) PutWithEvent(ctx context.Context, ldbKey []byte, value []byte,
	eventType EventType, eventCmd EventCommand) {

	txn.seq++
	event := &Event{
		Seq:  txn.seq,
		Type: eventType,
		Cmd:  eventCmd,
		Key:  ldbKey,
	}

	txn.batch.Put(ldbKey, value)
	txn.batch.Put(encodeEventKey(event.Seq), event.MarshalBinary())
}

func (txn *Txn) Delete(ctx context.Context, ldbKey []byte) {
	txn.batch.Delete(ldbKey)
}

func (txn *Txn) DeleteWithEvent(ctx context.Context, ldbKey []byte,
	eventType EventType, eventCmd EventCommand) {

	txn.seq++
	event := &Event{
		Seq:  txn.seq,
		Type: eventType,
		Cmd:  eventCmd,
		Key:  ldbKey,
	}

	txn.batch.Delete(ldbKey)
	txn.batch.Put(encodeEventKey(event.Seq), event.MarshalBinary())
}

func (txn *Txn) Rollback() {
	txn.batch.Reset()
	txn.mu.Unlock()
}

func (txn *Txn) Commit() error {
	if err := txn.ldb.Write(txn.batch, nil); err != nil {
		return err
	}

	txn.binlog.seq = txn.seq
	return nil
}
