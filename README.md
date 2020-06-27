raft-badgerdb
=============

A copy of [raft-boltdb](https://github.com/hashicorp/raft-boltdb) but with [BadgerDB](https://github.com/dgraph-io/badger) instead. BoltDB's buckets are replaced with using key prefixes in BadgerDB (this seems to be common). Reverse iterator is achieved with the suggestion here: https://github.com/dgraph-io/badger/issues/436#issuecomment-398904773

```go
opts := badger.DefaultIteratorOptions
opts.PrefetchValues = false
opts.Reverse = true

prefix := dbLogs
it := txn.NewIterator(opts)
defer it.Close()
it.Rewind()
if it.ValidForPrefix(prefix) {
        k := it.Item().Key()
        ret = bytesToUint64(bytes.TrimPrefix(k, dbLogs))
        return nil
}
```

### Bench

First generate results from this repo (and replace Badger with Bolt to be able to use benchcmp):

```
sevagh:raft-badgerdb $ go test -run=^a -bench=Bench* > raft-badgerdb-results.txt
sevagh:raft-badgerdb $ sed -i 's/Badger/Bolt/g' raft-badgerdb-results.txt
```

Then clone raft-boltdb and run those benchmarks:

```
sevagh:raft-boltdb $ go test -run=^a -bench=Bench* > raft-boltdb-results.txt
sevagh:raft-boltdb $ benchcmp ./raft-boltdb-results.txt ../raft-badgerdb/raft-badgerdb-results.txt
benchmark                            old ns/op     new ns/op     delta
BenchmarkBoltStore_FirstIndex-8      560           1432          +155.71%
BenchmarkBoltStore_LastIndex-8       508           1462          +187.80%
BenchmarkBoltStore_GetLog-8          1958          2657          +35.70%
BenchmarkBoltStore_StoreLog-8        24056         19920         -17.19%
BenchmarkBoltStore_StoreLogs-8       27322         49514         +81.22%
BenchmarkBoltStore_DeleteRange-8     26377         2265373       +8488.44%
BenchmarkBoltStore_Set-8             14993         22139         +47.66%
BenchmarkBoltStore_Get-8             766           1103          +43.99%
BenchmarkBoltStore_SetUint64-8       15952         15996         +0.28%
BenchmarkBoltStore_GetUint64-8       747           853           +14.19%
```

I wouldn't take the numbers seriously, given I may have made some suboptimal implementation choices.
