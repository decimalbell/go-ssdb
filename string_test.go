package ssdb

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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
		assert.Nil(t, err)
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

func TestSet(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()
	key := []byte("key")
	value := []byte("value")

	err := db.Set(ctx, key, value)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, db.binlog.Seq())

	seq := db.binlog.Seq()
	event, err := db.binlog.Get(ctx, seq)
	assert.Nil(t, err)
	assert.Equal(t, StringSet, event.Cmd)
	assert.EqualValues(t, encodeStringKey(key), event.Key)
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
		assert.Nil(t, err)
		assert.Nil(t, val)
	}
}

func TestDelBinlog(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()
	key := []byte("key")
	value := []byte("value")

	{
		err := db.Set(ctx, key, value)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, db.binlog.Seq())

		seq := db.binlog.Seq()
		event, err := db.binlog.Get(ctx, seq)
		assert.Nil(t, err)
		assert.Equal(t, StringSet, event.Cmd)
		assert.EqualValues(t, encodeStringKey(key), event.Key)
	}

	{
		err := db.Del(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 2, db.binlog.Seq())

		seq := db.binlog.Seq()
		event, err := db.binlog.Get(ctx, seq)
		assert.Nil(t, err)
		assert.Equal(t, StringDel, event.Cmd)
		assert.EqualValues(t, encodeStringKey(key), event.Key)
	}

}

func TestIncrby(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()

	{
		key := []byte("key1")
		value := []byte("value")

		err := db.Set(ctx, key, value)
		assert.Nil(t, err)

		val, err := db.Incrby(ctx, key, 10)
		assert.NotNil(t, err)
		assert.EqualValues(t, 0, val)
	}

	{
		key := []byte("key2")
		value, err := db.Incrby(ctx, key, 10)
		assert.Nil(t, err)
		assert.EqualValues(t, 10, value)

		value, err = db.Incrby(ctx, key, 10)
		assert.Nil(t, err)
		assert.EqualValues(t, 20, value)

		value, err = db.Incrby(ctx, key, -10)
		assert.Nil(t, err)
		assert.EqualValues(t, 10, value)

		val, err := db.Get(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, []byte("10"), val)
	}
}

func TestIncrbyParallel(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()
	key := []byte("key")
	count := 100

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			_, err := db.Incrby(ctx, key, 1)
			assert.Nil(t, err)
		}(i)
	}
	wg.Wait()

	value, err := db.Get(ctx, key)
	assert.Nil(t, err)
	assert.EqualValues(t, []byte(strconv.Itoa(count)), value)
}

func TestIncrParallel(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	ctx := context.TODO()
	key := []byte("key")
	count := 100

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			_, err := db.Incr(ctx, key)
			assert.Nil(t, err)
		}(i)
	}
	wg.Wait()

	value, err := db.Get(ctx, key)
	assert.Nil(t, err)
	assert.EqualValues(t, []byte(strconv.Itoa(count)), value)
}

func BenchmarkSet(b *testing.B) {
	dir, _ := os.MkdirTemp("", "ssdb")
	defer os.RemoveAll(dir)

	db, _ := Open(dir, nil)
	defer db.Close()

	keys := make([][]byte, 0, b.N)
	values := make([][]byte, 0, b.N)

	for i := 0; i < b.N; i++ {
		keys = append(keys, []byte(strconv.Itoa(i)))
		values = append(values, []byte(strconv.Itoa(i*2)))
	}

	ctx := context.TODO()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Set(ctx, keys[i], values[i])
		if err != nil {
			b.Errorf("Set: %v", err)
		}
	}
}
