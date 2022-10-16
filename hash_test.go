package ssdb

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestHGet(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	field := []byte("key")

	{
		val, err := db.HGet(ctx, key, field)
		assert.Equal(t, err, leveldb.ErrNotFound)
		assert.Nil(t, val)
	}
}

func TestHLen(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")

	{
		len, err := db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 0, len)
	}
}
