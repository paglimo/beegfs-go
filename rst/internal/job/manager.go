package job

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"path"
	"reflect"

	badger "github.com/dgraph-io/badger/v4"
	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	br "github.com/thinkparq/protobuf/beeremote/go"
	bs "github.com/thinkparq/protobuf/beesync/go"
	"go.uber.org/zap"
)

type Config struct {
	DBPath string `mapstructure:"dbPath"`
}

type Manager struct {
	log    *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	Config
	errChan chan<- error
}

func NewManager(log *zap.Logger, config Config, errCh chan<- error) *Manager {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{log: log, Config: config, errChan: errCh, ctx: ctx, cancel: cancel}
}

func (m *Manager) Manage() {

	db, err := badger.Open(badger.DefaultOptions(m.DBPath))
	if err != nil {
		m.errChan <- fmt.Errorf("unable to open database file at %s: %w", m.DBPath, err)
		return
		// TODO: Consider how we want to handle this. Should we move this into the loop and keep retrying?
	}
	defer db.Close()

	// Create Jobs for testing:
	entry := beegfs.Entry{Path: "/some/path"}
	sj := bs.SyncJob{Entry: &entry}
	jr := br.JobRequest{Name: "test", Priority: 3, Type: &br.JobRequest_Sync{Sync: &sj}}
	j, _ := New(&jr)
	j.Allocate()

	jr2 := br.JobRequest{Name: "test2", Priority: 6, Type: &br.JobRequest_Sync{Sync: &sj}}
	j2, _ := New(&jr2)
	j2.Allocate()

	jobs := []Job{j, j2}

	// Encode Jobs to a byte slice so it can be stored in Badger:
	var jobsBuf bytes.Buffer
	gob.Register(&SyncJob{})
	gob.Register(&SyncRequest{})

	enc := gob.NewEncoder(&jobsBuf)
	if err := enc.Encode(&jobs); err != nil {
		m.log.Error("error encoding", zap.Error(err))
	}

	// Add the Jobs to the database:
	if err = db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(j.GetPath()), jobsBuf.Bytes())
		return err
	}); err != nil {
		m.log.Error("error updating database", zap.Error(err))
	}

	// Retrieve the jobs from the database:
	var retrievedJobs []Job
	if err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("/some/path"))
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			dec := gob.NewDecoder(bytes.NewReader(val))
			err := dec.Decode(&retrievedJobs)
			return err
		})

		return err

	}); err != nil {
		m.log.Error("error getting from database", zap.Error(err))
	}

	// Verify jobs were properly retrieved:
	m.log.Info("retrieved job", zap.Any("jobs", retrievedJobs))
	retrievedJobs[0].Allocate()
	retrievedJobs[1].Allocate()

	m.errChan <- errors.New("shutdown for testing")

}

func (m *Manager) Stop() {
	m.cancel()
}
