package server

import (
	"context"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("storage.Reader() failed, err:%+v", err)
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.RawGetResponse{}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		if err == badger.ErrKeyNotFound {
			resp.NotFound = true
			return resp, nil
		} else {
			log.Errorf("reader.GetCF() failed, err:%+v", err)
			return nil, err
		}
	}

	resp.Value = value
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	ctx := req.GetContext()
	modify := storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}

	resp := &kvrpcpb.RawPutResponse{}
	err := server.storage.Write(ctx, []storage.Modify{modify})
	if err != nil {
		log.Errorf("storage.Write() failed, modify:%+v", modify)
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	ctx := req.GetContext()
	modify := storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}

	resp := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(ctx, []storage.Modify{modify})
	if err != nil {
		log.Errorf("storage.Write() failed, modify:%+v", modify)
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	ctx := req.GetContext()
	reader, err := server.storage.Reader(ctx)
	if err != nil {
		log.Errorf("storage.Reader() failed, err:%s", err)
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.RawScanResponse{}
	it := reader.IterCF(req.GetCf())
	defer it.Close()

	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			log.Errorf("item.ValueCopy() failed, err:%s", err)
			return nil, err
		}
		kv := &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		}
		resp.Kvs = append(resp.Kvs, kv)
		if uint32(len(resp.Kvs)) >= req.Limit {
			break
		}
	}
	return resp, nil
}
