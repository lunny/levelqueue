// Copyright 2019 Lunny Xiao. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package levelqueue

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	os.RemoveAll("./queue")
	queue, err := New("./queue")
	assert.NoError(t, err)

	err = queue.RPush([]byte("test"))
	assert.NoError(t, err)

	assert.EqualValues(t, 1, queue.Len())

	data, err := queue.LPop()
	assert.NoError(t, err)
	assert.EqualValues(t, "test", string(data))

	// should be empty
	data, err = queue.LPop()
	assert.Error(t, err)
	assert.EqualValues(t, []byte(nil), data)
	assert.EqualValues(t, ErrNotFound, err)

	assert.EqualValues(t, 0, queue.Len())

	err = queue.LPush([]byte("test2"))
	assert.NoError(t, err)

	data, err = queue.LPop()
	assert.NoError(t, err)
	assert.EqualValues(t, "test2", string(data))

	assert.EqualValues(t, 0, queue.Len())

	data, err = queue.LPop()
	assert.Error(t, err)
	assert.EqualValues(t, []byte(nil), data)
	assert.EqualValues(t, ErrNotFound, err)

	data, err = queue.RPop()
	assert.Error(t, err)
	assert.EqualValues(t, []byte(nil), data)
	assert.EqualValues(t, ErrNotFound, err)
}
