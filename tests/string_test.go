package ssdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/decimalbell/go-ssdb"
	"github.com/stretchr/testify/assert"
)

const (
	dir = "../testdata/var/data"
)

func TestGet(t *testing.T) {
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
