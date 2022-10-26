package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/decimalbell/go-ssdb"
	"github.com/stretchr/testify/assert"
)

func TestZCard(t *testing.T) {
	db, err := ssdb.Open(dir, nil)
	assert.Nil(t, err)
	defer db.Close()

	ctx := context.TODO()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("zset%d", i))
		val, err := db.ZCard(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 10, val)
	}
}

func TestZScore(t *testing.T) {
	db, err := ssdb.Open(dir, nil)
	assert.Nil(t, err)
	defer db.Close()

	ctx := context.TODO()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("zset%d", i))
		for j := 0; j < 10; j++ {
			member := []byte(fmt.Sprintf("member%d", j))
			val, err := db.ZScore(ctx, key, member)
			assert.Nil(t, err)
			assert.EqualValues(t, j, val)
		}
	}
}
