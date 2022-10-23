package ssdb

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinlogGet(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()
	event, err := db.binlog.Get(ctx, 1)
	assert.NotNil(t, err)
	assert.Nil(t, event)
}
