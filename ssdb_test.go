package ssdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	dir, err := os.MkdirTemp("", "ssdb")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(dir, nil)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	defer db.Close()
}

func TestClose(t *testing.T) {
	db := &DB{}
	defer db.Close()
}
