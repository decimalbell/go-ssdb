package ssdb

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHGet(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	field := []byte("field")
	value := []byte("value")

	{
		actualValue, err := db.HGet(ctx, key, field)
		assert.Nil(t, err)
		assert.Nil(t, actualValue)
	}

	{
		err := db.HSet(ctx, key, field, value)
		assert.Nil(t, err)

		actualValue, err := db.HGet(ctx, key, field)
		assert.Nil(t, err)
		assert.Equal(t, value, actualValue)
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

func TestHSetParallel(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()
	key := []byte("key")
	count := 100
	fields := make([][]byte, 0, count)
	values := make([][]byte, 0, count)

	for i := 0; i < 100; i++ {
		field := []byte(strconv.Itoa(i))
		value := []byte(strconv.Itoa(i))

		fields = append(fields, field)
		values = append(values, value)
	}

	{
		var wg sync.WaitGroup
		for i := 0; i < count; i++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				err := db.HSet(ctx, key, fields[i], values[i])
				assert.Nil(t, err)
			}(i)
		}
		wg.Wait()

		len, err := db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, count, len)
	}

	{
		var wg sync.WaitGroup
		for i := 0; i < count; i++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				ok, err := db.HDel(ctx, key, fields[i])
				assert.Nil(t, err)
				assert.Equal(t, true, ok)
			}(i)
		}
		wg.Wait()

		len, err := db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 0, len)
	}
}

func TestHSetNX(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	field := []byte("field")
	value := []byte("value")

	{
		ok, err := db.HSetNX(ctx, key, field, value)
		assert.Nil(t, err)
		assert.Equal(t, true, ok)

		len, err := db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len)

		ok, err = db.HSetNX(ctx, key, field, value)
		assert.Nil(t, err)
		assert.Equal(t, false, ok)

		len, err = db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len)
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

func TestHDel(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	field := []byte("field")
	value := []byte("value")

	{
		ok, err := db.HDel(ctx, key, field)
		assert.Nil(t, err)
		assert.EqualValues(t, false, ok)
	}

	{
		err := db.HSet(ctx, key, field, value)
		assert.Nil(t, err)

		len, err := db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len)

		ok, err := db.HDel(ctx, key, field)
		assert.Nil(t, err)
		assert.EqualValues(t, true, ok)

		len, err = db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 0, len)
	}
}

func TestHKeys(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	fields := make([][]byte, 0, 32)
	for i := 0; i < 100; i++ {
		field := []byte(strconv.Itoa(i))
		value := []byte(strconv.Itoa(i * 2))
		err := db.HSet(ctx, key, field, value)
		assert.Nil(t, err)

		fields = append(fields, field)
	}
	actualFields, err := db.HKeys(ctx, key)
	assert.Nil(t, err)
	assert.ElementsMatch(t, fields, actualFields)
}

func TestHVals(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	values := make([][]byte, 0, 32)
	for i := 0; i < 100; i++ {
		field := []byte(strconv.Itoa(i))
		value := []byte(strconv.Itoa(i * 2))
		err := db.HSet(ctx, key, field, value)
		assert.Nil(t, err)

		values = append(values, value)
	}
	actualValues, err := db.HVals(ctx, key)
	assert.Nil(t, err)
	assert.ElementsMatch(t, values, actualValues)
}

func TestHGetAll(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	all := make([][]byte, 0, 32)
	for i := 0; i < 100; i++ {
		field := []byte(strconv.Itoa(i))
		value := []byte(strconv.Itoa(i * 2))
		err := db.HSet(ctx, key, field, value)
		assert.Nil(t, err)

		all = append(all, field, value)
	}
	actualAll, err := db.HGetAll(ctx, key)
	assert.Nil(t, err)
	assert.ElementsMatch(t, all, actualAll)
}

func TestHStrLen(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	key := []byte("key")
	field := []byte("field")
	value := []byte("value")

	{
		value, err := db.HStrLen(ctx, key, field)
		assert.Nil(t, err)
		assert.Equal(t, 0, value)
	}

	{
		err := db.HSet(ctx, key, field, value)
		assert.Nil(t, err)

		strLen, err := db.HStrLen(ctx, key, field)
		assert.Nil(t, err)
		assert.Equal(t, len(value), strLen)
	}
}
