// Copyright 2019 Lunny Xiao. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package levelqueue

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

// Queue defines a queue struct
type Queue struct {
	db       *leveldb.DB
	highLock sync.Mutex
	lowLock  sync.Mutex
	low      int64
	high     int64
}

func (queue *Queue) readID(key []byte) (int64, error) {
	bs, err := queue.db.Get(key, nil)
	if err != nil {
		return 0, err
	}
	return bytes2id(bs)
}

// New creates a new queue object
func New(dataDir string) (*Queue, error) {
	db, err := leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return nil, err
	}

	var queue = &Queue{
		db: db,
	}
	queue.low, err = queue.readID(lowKey)
	if err == leveldb.ErrNotFound {
		queue.low = 1
		err = db.Put(lowKey, id2bytes(1), nil)
	}
	if err != nil {
		return nil, err
	}

	queue.high, err = queue.readID(highKey)
	if err == leveldb.ErrNotFound {
		err = db.Put(highKey, id2bytes(0), nil)
	}
	if err != nil {
		return nil, err
	}

	return queue, nil
}

var (
	lowKey  = []byte("low")
	highKey = []byte("high")
)

func (queue *Queue) highincrement() (int64, error) {
	queue.highLock.Lock()
	queue.high = queue.high + 1
	err := queue.db.Put(highKey, id2bytes(queue.high), nil)
	if err != nil {
		queue.high = queue.high - 1
		queue.highLock.Unlock()
		return 0, err
	}
	queue.highLock.Unlock()
	return queue.high, nil
}

func (queue *Queue) highdecrement() (int64, error) {
	queue.highLock.Lock()
	queue.high = queue.high - 1
	err := queue.db.Put(highKey, id2bytes(queue.high), nil)
	if err != nil {
		queue.high = queue.high + 1
		queue.highLock.Unlock()
		return 0, err
	}
	queue.highLock.Unlock()
	return queue.high, nil
}

func (queue *Queue) lowincrement() (int64, error) {
	queue.lowLock.Lock()
	queue.low = queue.low + 1
	err := queue.db.Put(lowKey, id2bytes(queue.low), nil)
	if err != nil {
		queue.low = queue.low - 1
		queue.lowLock.Unlock()
		return 0, err
	}
	queue.lowLock.Unlock()
	return queue.low, nil
}

func (queue *Queue) lowdecrement() (int64, error) {
	queue.lowLock.Lock()
	queue.low = queue.low - 1
	err := queue.db.Put(lowKey, id2bytes(queue.low), nil)
	if err != nil {
		queue.low = queue.low + 1
		queue.lowLock.Unlock()
		return 0, err
	}
	queue.lowLock.Unlock()
	return queue.low, nil
}

func (queue *Queue) Len() int64 {
	queue.lowLock.Lock()
	queue.highLock.Lock()
	l := queue.high - queue.low + 1
	queue.highLock.Unlock()
	queue.lowLock.Unlock()
	return l
}

func id2bytes(id int64) []byte {
	var buf = make([]byte, 8)
	binary.PutVarint(buf, id)
	return buf
}

func bytes2id(b []byte) (int64, error) {
	return binary.ReadVarint(bytes.NewReader(b))
}

func (queue *Queue) RPush(data []byte) error {
	id, err := queue.highincrement()
	if err != nil {
		return err
	}
	return queue.db.Put(id2bytes(id), data, nil)
}

func (queue *Queue) LPush(data []byte) error {
	id, err := queue.lowdecrement()
	if err != nil {
		return err
	}
	return queue.db.Put(id2bytes(id), data, nil)
}

func (queue *Queue) RPop() ([]byte, error) {
	currentID := queue.high
	res, err := queue.db.Get(id2bytes(currentID), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}

	_, err = queue.highdecrement()
	if err != nil {
		return nil, err
	}

	err = queue.db.Delete(id2bytes(currentID), nil)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (queue *Queue) LPop() ([]byte, error) {
	currentID := queue.low
	res, err := queue.db.Get(id2bytes(currentID), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}

	_, err = queue.lowincrement()
	if err != nil {
		return nil, err
	}

	err = queue.db.Delete(id2bytes(currentID), nil)
	if err != nil {
		return nil, err
	}
	return res, nil
}
