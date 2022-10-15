package ssdb

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestEncodeStringKey(t *testing.T) {
	assert.Equal(t, []byte("kkey"), encodeStringKey([]byte("key")))
}

func TestGet(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	value := []byte("value")

	{
		val, err := db.Get(ctx, key)
		assert.Equal(t, err, leveldb.ErrNotFound)
		assert.Nil(t, val)
	}

	{
		err := db.Set(ctx, key, value)
		assert.Nil(t, err)

		val, err := db.Get(ctx, key)
		assert.Nil(t, err)
		assert.Equal(t, value, val)
	}
}

func TestDel(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	value := []byte("value")

	{
		err := db.Del(ctx, key)
		assert.Nil(t, err)
	}

	{
		err := db.Set(ctx, key, value)
		assert.Nil(t, err)

		val, err := db.Get(ctx, key)
		assert.Nil(t, err)
		assert.Equal(t, value, val)

		err = db.Del(ctx, key)
		assert.Nil(t, err)

		val, err = db.Get(ctx, key)
		assert.Equal(t, leveldb.ErrNotFound, err)
		// assert.Nil(t, val)
		assert.Equal(t, []byte(""), val)
	}
}
