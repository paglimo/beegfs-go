package job

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"path"
	"reflect"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/bee-remote/internal/worker"
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

func (m *Manager) Manage(jobSubmissions chan<- worker.JobSubmission, jobResults <-chan worker.JobResult) {

	db, err := badger.Open(badger.DefaultOptions(m.DBPath))
	if err != nil {
		m.errChan <- fmt.Errorf("unable to open database file at %s: %w", m.DBPath, err)
		return
		// TODO: Consider how we want to handle this. Should we move this into the loop and keep retrying?
	}
	defer db.Close()

	// TODO: Evaluate if a bandwidth of 1000 is appropriate.
	// Probably this should be user configurable.
	// Ref: https://dgraph.io/docs/badger/get-started/#monotonically-increasing-integers
	jobSeq, err := db.GetSequence([]byte("key"), 1000)
	if err != nil {
		m.log.Error("error setting up job sequence generation", zap.Error(err))
		m.errChan <- err
	}
	defer jobSeq.Release()

	// Create Jobs for testing:
	sj := bs.SyncJob{}
	jr := br.JobRequest{Path: "/some/path", Name: "test", Priority: 3, Type: &br.JobRequest_Sync{Sync: &sj}}
	j, _ := New(jobSeq, &jr)
	j.Allocate()

	jr2 := br.JobRequest{Path: "/some/path", Name: "test2", Priority: 6, Type: &br.JobRequest_Sync{Sync: &sj}}
	j2, _ := New(jobSeq, &jr2)
	j2.Allocate()

	jobs := []Job{j, j2}

	// Encode Jobs to a byte slice so it can be stored in Badger:
	var jobsBuf bytes.Buffer
	gob.Register(&SyncJob{})
	gob.Register(&SyncSegment{})

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

	// Listen for responses in a separate goroutine.
	go func() {
		for i := 0; i < 2; i++ {
			select {
			case <-m.ctx.Done():
				return
			case r := <-jobResults:
				m.log.Info("received response", zap.Any("response", r))
			}
		}
	}()

	// Test submitting jobs.
	jobSubmissions <- retrievedJobs[0].Allocate()
	jobSubmissions <- retrievedJobs[1].Allocate()

	// TODO: For testing wait a bit otherwise we'll exit before responses can be
	// sent. Don't forget to remove later.
	<-time.After(time.Duration(3) * time.Second)
	m.errChan <- errors.New("automatically shutdown after test completion")
}

func (m *Manager) Stop() {
	m.cancel()
}
