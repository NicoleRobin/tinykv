package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	s := &StandAloneStorage{
		db: db,
	}
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// #TODO: 是应该在new的时候打开db呢，还是在start的时候？
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &standaloneReader{
		inner: s,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			key := engine_util.KeyWithCF(m.Cf(), m.Key())
			switch m.Data.(type) {
			case storage.Put:
				err := txn.Set(key, m.Value())
				if err != nil {
					log.Errorf("txn.Set() failed, key:%s, value:%+v", key, m.Value())
					return err
				}
			case storage.Delete:
				err := txn.Delete(key)
				if err != nil {
					log.Errorf("txn.Set() failed, key:%s", key)
					return err
				}
			default:
				log.Errorf("invalid operator")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// standaloneReader is a StorageReader which reads from a StandaloneStorage.
type standaloneReader struct {
	inner *StandAloneStorage
	txn   *badger.Txn
}

func (s *standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	var value []byte
	err := s.inner.db.View(func(txn *badger.Txn) (err error) {
		value, err = engine_util.GetCFFromTxn(txn, cf, key)
		return err
	})
	if err != nil {
		log.Errorf("db.View() failed, err:%+v", err)
		return nil, err
	}
	return value, nil
}

func (s *standaloneReader) IterCF(cf string) engine_util.DBIterator {
	// 由于这里会把it传递出去，因此不能使用db.View()方法，因为该方法执行结束时txn就会被关闭了。
	s.txn = s.inner.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *standaloneReader) Close() {
	if s.txn != nil {
		s.txn.Discard()
		s.txn = nil
	}
}
