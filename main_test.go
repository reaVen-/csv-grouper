package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindOffset(t *testing.T) {
	f, err := os.Open("go.csv")
	assert.Nil(t, err)

	offsets := findOffsets(f, 887374425, 16)

	for x := 1; x < len(offsets); x++ {
		//make sure offset-1 is \n so that you start reading from a new record
		assert.Equal(t, "\n", peekByte(f, offsets[x]-1))
	}
}

func peekByte(f *os.File, offset int) string {
	buf := make([]byte, 1)
	i, err := f.ReadAt(buf, int64(offset))
	if err != nil || i != 1 {
		panic("no byte found")
	}
	return string(buf[0])
}
