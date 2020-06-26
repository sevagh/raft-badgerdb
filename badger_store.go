package raftbadgerdb

import (
	"errors"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/hashicorp/raft"
)

var (
	// key prefixes to denote buckets
	dbLogs = []byte("logs")
	dbConf = []byte("conf")

	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// BadgerStore provides access to BadgerDB for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type BadgerStore struct {
	// conn is the underlying handle to the db.
	conn *badger.DB

	// The path to the Badger database file
	path string
}

// NewBadgerStore takes a file path and returns a connected Raft backend.
func NewBadgerStore(path string) (*BadgerStore, error) {
	return New(badger.DefaultOptions(path))
}

// New uses the supplied options to open the BadgerDB and prepare it for use as a raft backend.
func New(options badger.Options) (*BadgerStore, error) {
	handle, err := badger.Open(options)
	if err != nil {
		return nil, err
	}

	store := &BadgerStore{
		conn: handle,
		path: options.Dir,
	}

	return store, nil
}

// Close is used to gracefully close the DB connection.
func (b *BadgerStore) Close() error {
	return b.conn.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (b *BadgerStore) FirstIndex() (uint64, error) {
	tx := b.conn.NewTransaction(false)
	defer tx.Discard()

	itOpts := badger.DefaultIteratorOptions
	itOpts.Prefix = dbLogs

	iterator := tx.NewIterator(itOpts)
	defer iterator.Close()

	// check if the iterator is empty
	if !iterator.Valid() {
		return 0, nil
	}

	var ret uint64

	// get the value thru an anonymous function without incurring
	// copy cost
	if firstItem := iterator.Item(); firstItem != nil {
		firstItem.Value(func(val []byte) error {
			ret = bytesToUint64(val)
			return nil
		})
	}
	return ret, nil
}

// LastIndex returns the last known index from the Raft log.
func (b *BadgerStore) LastIndex() (uint64, error) {
	tx := b.conn.NewTransaction(false)
	defer tx.Discard()

	itOpts := badger.DefaultIteratorOptions
	itOpts.Prefix = dbLogs

	iterator := tx.NewIterator(itOpts)
	defer iterator.Close()

	var ret uint64
	for {
		if !iterator.Valid() {
			break
		}

		if item := iterator.Item(); item != nil {
			// get the value thru an anonymous function without incurring
			// copy cost
			item.Value(func(val []byte) error {
				ret = bytesToUint64(val)
				return nil
			})
		}

		iterator.Next()
	}
	return ret, nil
}

// GetLog is used to retrieve a log from BadgerDB at a given index.
func (b *BadgerStore) GetLog(idx uint64, log *raft.Log) error {
	tx := b.conn.NewTransaction(false)
	defer tx.Discard()

	// simulate boltdb "buckets" with key prefixes
	// dbLogs is the prefix of the log keys
	item, err := tx.Get(append(dbLogs, uint64ToBytes(idx)...))

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return raft.ErrLogNotFound
		}
		return err
	}

	if item == nil {
		return raft.ErrLogNotFound
	}

	item.Value(func(val []byte) error {
		decodeMsgPack(val, log)
		return nil
	})
	return nil
}

// StoreLog is used to store a single raft log
func (b *BadgerStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (b *BadgerStore) StoreLogs(logs []*raft.Log) error {
	tx := b.conn.NewTransaction(true)
	defer tx.Discard()

	for _, log := range logs {
		// once again, simulate boltdb "buckets" with key prefixes
		// dbLogs is the prefix of the log keys
		key := append(dbLogs, uint64ToBytes(log.Index)...)

		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}

		// it is not allowed to re-use key and value in the same
		// transcation. however we seem to be allocating new slices
		// so...
		if err := tx.Set(key, val.Bytes()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *BadgerStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)

	tx := b.conn.NewTransaction(true)
	defer tx.Discard()

	itOpts := badger.DefaultIteratorOptions
	itOpts.Prefix = dbLogs

	iterator := tx.NewIterator(itOpts)
	defer iterator.Close()

	// deal with the minKey first
	iterator.Seek(minKey)
	for {
		if !iterator.Valid() {
			break
		}

		if item := iterator.Item(); item != nil {
			// Delete in-range log index
			k := item.Key()

			if bytesToUint64(k) > max {
				break
			}
			if err := tx.Delete(k); err != nil {
				return err
			}
		}

		iterator.Next()
	}

	return tx.Commit()
}

// Set is used to set a key/value set outside of the raft log
func (b *BadgerStore) Set(k, v []byte) error {
	tx := b.conn.NewTransaction(true)
	defer tx.Discard()

	if err := tx.Set(append(dbConf, k...), v); err != nil {
		return err
	}

	return tx.Commit()
}

// Get is used to retrieve a value from the k/v store by key
func (b *BadgerStore) Get(k []byte) ([]byte, error) {
	tx := b.conn.NewTransaction(false)
	defer tx.Discard()

	item, err := tx.Get(append(dbConf, k...));
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, raft.ErrLogNotFound
		}
		return nil, err
	}

	if item == nil {
		return nil, ErrKeyNotFound
	}

	return item.ValueCopy(nil)
}

// SetUint64 is like Set, but handles uint64 values
func (b *BadgerStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (b *BadgerStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// Sync performs an fsync on the database file handle. This is not necessary
// under normal operation unless NoSync is enabled, in which this forces the
// database file to sync against the disk.
func (b *BadgerStore) Sync() error {
	return b.conn.Sync()
}
