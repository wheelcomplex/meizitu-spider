//

// taskdb impl task queue
package taskdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wheelcomplex/bolt"
)

// Taskdb
type Taskdb struct {
	db      *bolt.DB   // the db
	maxurl  int        //
	bkname  []byte     //
	seqbuf  []byte     //
	taskbuf []byte     //
	m       sync.Mutex // the locker
}

// UrlTask
type UrlTask struct {
	Seq uint64 //
	Url []byte //
}

//
func (t UrlTask) String() string {
	return fmt.Sprintf("url task(%d)#%d=%s", len(t.Url), t.Seq, t.Url)
}

//
const MaxUrl = 2048

// error list
var (
	ErrNoTask = errors.New("Task queue is empty")
)

// NewDefaultTaskdb return db with default config
func NewDefaultTaskdb() (*Taskdb, error) {
	var err error
	defdbname := "task.default.db"
	defbkname := "task.default.url"
	tdb, err := NewTaskdb(defdbname, defbkname, MaxUrl)
	if err != nil {
		err = fmt.Errorf("error: initial default task db %s/%s failed: %s\n", defdbname, defbkname, err.Error())
		return nil, err
	}
	return tdb, nil
}

// NewTaskdb initial dbfile with 5 seconds timeout
func NewTaskdb(dbfile string, bucketname string, maxurl int) (*Taskdb, error) {
	if maxurl < 16 {
		maxurl = 16
	}
	db, err := bolt.Open(dbfile, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	bkname := []byte(bucketname)
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bkname)
		if err != nil {
			return fmt.Errorf("create bucket %s in %s: %s", bucketname, dbfile, err)
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	bb := &Taskdb{
		db:      db,
		bkname:  bkname,
		maxurl:  maxurl,
		seqbuf:  make([]byte, 8),
		taskbuf: make([]byte, maxurl+8),
	}
	return bb, nil
}

// seq2buf convert v into big endian representation and save in internal buf,
// it's thread unsafe.
func (bb *Taskdb) seq2buf(task *UrlTask) []byte {
	binary.BigEndian.PutUint64(bb.seqbuf, task.Seq)
	return bb.seqbuf
}

// task2buf convert v into []byte representation and save in internal buf,
//
// note: truncat when task.Url is large then 1024 byte
// it's thread unsafe.
func (bb *Taskdb) task2buf(task *UrlTask) []byte {
	binary.BigEndian.PutUint64(bb.taskbuf, task.Seq)
	end := len(task.Url) + 8
	if end > bb.maxurl {
		end = bb.maxurl
	}
	copy(bb.taskbuf[8:end], task.Url)
	return bb.taskbuf[:end]
}

// buf2seq
//
// incorrect buf will return incorrect seq.
//
// it's thread unsafe.
func (bb *Taskdb) buf2seq(task *UrlTask, buf []byte) {
	copy(bb.seqbuf, buf)
	task.Seq = binary.BigEndian.Uint64(bb.seqbuf)
	return
}

// buf2task
//
// incorrect buf will return incorrect task.
//
// it's thread unsafe.
func (bb *Taskdb) buf2task(task *UrlTask, buf []byte) {
	copy(bb.taskbuf, buf)
	task.Seq = binary.BigEndian.Uint64(bb.taskbuf[:8])
	end := len(buf) - 8
	if end > len(task.Url) {
		end = len(task.Url)
	}
	copy(task.Url, bb.taskbuf[8:end])
	return
}

// Close
func (bb *Taskdb) Close() error {
	bb.m.Lock()
	err := bb.db.Close()
	bb.m.Unlock()
	return err
}

// SaveTask store url into db(FIFO)
func (bb *Taskdb) SaveTask(task *UrlTask) error {
	bb.m.Lock()
	// write only
	err := bb.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bb.bkname)
		// Generate Seq for the task.
		// This returns an error only if the Tx is closed or not writeable.
		// That can't happen in an Update() call so I ignore the error check.
		task.Seq, _ = b.NextSequence()

		// Persist bytes to users bucket.
		return b.Put(bb.seq2buf(task), bb.task2buf(task))
	})
	bb.m.Unlock()
	return err
}

// GetTask return first url from db(FIFO)
func (bb *Taskdb) GetTask(task *UrlTask) error {
	bb.m.Lock()
	// read task and delete from db
	err := bb.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bb.bkname)

		c := b.Cursor()

		/*
		   First()  Move to the first key.
		   Last()   Move to the last key.
		   Seek()   Move to a specific key.
		   Next()   Move to the next key.
		   Prev()   Move to the previous key.
		*/
		k, v := c.First()
		for ; k != nil; k, v = c.Next() {
			//k, v := c.Last()
			//for ; k != nil; k, v = c.Prev() {
			break
		}
		if k == nil || len(v) < 8 {
			// bucket is empty
			return ErrNoTask
		}
		bb.buf2task(task, v)
		fmt.Printf("Get First, key=%v(%d), value=%v(%s)\n", k, task.Seq, v[8:], v[8:])
		return nil
	})
	bb.m.Unlock()
	return err
}

// GetTaskbySeq return task from db, matched by task.Seq
func (bb *Taskdb) GetTaskBySeq(task *UrlTask) error {
	bb.m.Lock()
	// read task and delete from db
	err := bb.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bb.bkname)

		k := bb.seq2buf(task)
		v := b.Get(k)

		if k == nil || len(v) < 8 {
			// bucket is empty
			return ErrNoTask
		}
		bb.buf2task(task, v)
		fmt.Printf("Get by seq, key=%v(%d), value=%v(%s)\n", k, task.Seq, v[8:], v[8:])
		return nil
	})
	bb.m.Unlock()
	return err
}

// PopTask return first url from db(FIFO), and remove from db
func (bb *Taskdb) PopTask(task *UrlTask) error {
	bb.m.Lock()
	// read task and delete from db
	err := bb.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bb.bkname)

		c := b.Cursor()

		/*
		   First()  Move to the first key.
		   Last()   Move to the last key.
		   Seek()   Move to a specific key.
		   Next()   Move to the next key.
		   Prev()   Move to the previous key.
		*/
		k, v := c.First()
		for ; k != nil; k, v = c.Next() {
			//k, v := c.Last()
			//for ; k != nil; k, v = c.Prev() {
			break
		}
		if k == nil || len(v) < 8 {
			// bucket is empty
			return ErrNoTask
		}
		bb.buf2task(task, v)
		fmt.Printf("Get First, key=%v(%d), value=%v(%s)\n", k, task.Seq, v[8:], v[8:])
		err := b.Delete(k)
		if err != nil {
			return err
		}
		return nil
	})
	bb.m.Unlock()
	return err
}

// PopTaskbySeq return task from db, matched by task.Seq, and remove from db
func (bb *Taskdb) PopTaskBySeq(task *UrlTask) error {
	bb.m.Lock()
	// read task and delete from db
	err := bb.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bb.bkname)

		k := bb.seq2buf(task)
		v := b.Get(k)

		if k == nil || len(v) < 8 {
			// bucket is empty
			return ErrNoTask
		}
		bb.buf2task(task, v)
		fmt.Printf("Get by seq, key=%v(%d), value=%v(%s)\n", k, task.Seq, v[8:], v[8:])
		err := b.Delete(k)
		if err != nil {
			return err
		}
		return nil
	})
	bb.m.Unlock()
	return err
}
