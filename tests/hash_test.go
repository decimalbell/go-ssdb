package ssdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/decimalbell/go-ssdb"
	"github.com/stretchr/testify/assert"
)

func TestHGet(t *testing.T) {
	db, err := ssdb.Open(dir, nil)
	assert.Nil(t, err)
	defer db.Close()

	ctx := context.TODO()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("hash%d", i))
		for j := 0; j < 1; j++ {
			field := []byte(fmt.Sprintf("field%d", j))
			value := []byte(fmt.Sprintf("value%d", j))
			val, err := db.HGet(ctx, key, field)
			assert.Nil(t, err)
			assert.Equal(t, value, val)
		}

		len, err := db.HLen(ctx, key)
		assert.Nil(t, err)
		assert.EqualValues(t, 10, len)
	}
}
