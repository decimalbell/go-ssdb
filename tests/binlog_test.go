package ssdb

import (
	"context"
	"testing"

	"github.com/decimalbell/go-ssdb"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestBinlogGet(t *testing.T) {
	ldb, err := leveldb.OpenFile(dir, nil)
	assert.Nil(t, err)
	defer ldb.Close()

	binlog := ssdb.NewBinlog(ldb)

	ctx := context.TODO()

	value, err := binlog.Get(ctx, 1)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value.Seq)
}
