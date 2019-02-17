// Copyright 2019 Lunny Xiao. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package levelqueue

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	const dbDir = "./queue"

	os.RemoveAll(dbDir)
	queue, err := Open(dbDir)
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

	err = queue.Close()
	assert.NoError(t, err)

	queue, err = Open(dbDir)
	assert.NoError(t, err)

	err = queue.RPush([]byte("test3"))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, queue.Len())

	data, err = queue.RPop()
	assert.NoError(t, err)
	assert.EqualValues(t, "test3", string(data))
}

func TestGoroutines(t *testing.T) {
	const dbDir = "./queue"

	os.RemoveAll(dbDir)
	queue, err := Open(dbDir)
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		err := queue.RPush([]byte("test"))
		assert.NoError(t, err)
	}

	var w sync.WaitGroup
	for i := 0; i < 10; i++ {
		w.Add(1)
		go func(i int) {
			if i%2 == 0 {
				err := queue.RPush([]byte("test"))
				assert.NoError(t, err)
			} else {
				_, err := queue.RPop()
				assert.NoError(t, err)
			}
			w.Done()
		}(i)
	}
	w.Wait()
}

func BenchmarkPush(b *testing.B) {
	const dbDir = "./queue_push"

	os.RemoveAll(dbDir)
	queue, err := Open(dbDir)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		err = queue.RPush([]byte("test"))
		assert.NoError(b, err)
	}
}

func BenchmarkPop(b *testing.B) {
	const dbDir = "./queue_pop"

	os.RemoveAll(dbDir)
	queue, err := Open(dbDir)
	assert.NoError(b, err)
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		err = queue.RPush([]byte("test"))
		assert.NoError(b, err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err = queue.RPop()
		assert.NoError(b, err)
	}
}
