package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/decimalbell/go-ssdb"
	"github.com/stretchr/testify/assert"
)

func TestLLen(t *testing.T) {
	db, err := ssdb.Open(dir, nil)
	assert.Nil(t, err)
	defer db.Close()

	ctx := context.TODO()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("list%d", i))
		val, err := db.LLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 20, val)
	}
}

func TestLIndex(t *testing.T) {
	db, err := ssdb.Open(dir, nil)
	assert.Nil(t, err)
	defer db.Close()

	ctx := context.TODO()

	for i := 0; i < 1; i++ {
		key := []byte(fmt.Sprintf("list%d", i))
		for j := 0; j < 10; j++ {
			element := []byte(fmt.Sprintf("element%d", 9-j))
			val, err := db.LIndex(ctx, key, int64(j))
			assert.Nil(t, err)
			assert.EqualValues(t, element, val)
		}

		for j := 10; j < 20; j++ {
			element := []byte(fmt.Sprintf("element%d", j-10))
			val, err := db.LIndex(ctx, key, int64(j))
			assert.Nil(t, err)
			assert.EqualValues(t, element, val)
		}

		for j := -1; j >= -10; j-- {
			element := []byte(fmt.Sprintf("element%d", 10+j))
			val, err := db.LIndex(ctx, key, int64(j))
			assert.Nil(t, err)
			assert.EqualValues(t, element, val)
		}

		for j := -11; j >= -20; j-- {
			element := []byte(fmt.Sprintf("element%d", -(11 + j)))
			val, err := db.LIndex(ctx, key, int64(j))
			assert.Nil(t, err)
			assert.EqualValues(t, element, val)
		}
	}
}
