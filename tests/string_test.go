package ssdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/decimalbell/go-ssdb"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	dir := "../testdata/var/data"

	db, err := ssdb.Open(dir, nil)
	assert.Nil(t, err)
	defer db.Close()

	ctx := context.TODO()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("string%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		val, err := db.Get(ctx, key)
		assert.Nil(t, err)
		assert.Equal(t, value, val)
	}
}
