# levelqueue

Level queue is a simple queue golang library base on go-leveldb.

## Installation

```
go get github.com/lunny/levelqueue
```

## Usage

```Go
queue, err := New("./queue")

err = queue.RPush([]byte("test"))

data, err = queue.LPop()
```