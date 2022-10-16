package ssdb

import (
	"context"
	"os"
	"strconv"
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
	field := []byte("field")

	{
		val, err := db.HGet(ctx, key, field)
		assert.Equal(t, err, leveldb.ErrNotFound)
		assert.Nil(t, val)
	}
}

func TestHSet(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	field := []byte("field")
	value := []byte("value")

	{
		err := db.HSet(ctx, key, field, value)
		assert.Nil(t, err)

		len, err := db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len)

		err = db.HSet(ctx, key, field, value)
		assert.Nil(t, err)

		len, err = db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len)
	}

	{
		key := []byte("k")
		for i := 0; i < 100; i++ {
			field := []byte(strconv.Itoa(i))
			value := []byte(strconv.Itoa(i))
			err := db.HSet(ctx, key, field, value)
			assert.Nil(t, err)
		}

		len, err := db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 100, len)
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
