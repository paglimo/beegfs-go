package worker

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	beegfs "github.com/thinkparq/protobuf/beegfs/go"
)

// WorkResult carries status and node assignment for a particular WorkRequest.
// It is setup so it can be copied when needed, either in JobResults or when
// saving WorkResults to disk. Because it doesn't contain a mutex it is not
// thread safe unless encapsulated in a workStoreEntry struct.
type WorkResult struct {
	RequestID string
	Status    beegfs.RequestStatus_Status
	Message   string
	// Assigned to indicates the node running this work request or "" if it is unassigned.
	AssignedTo string
}

// workStoreEntry contains the WorkResults of all WorkRequests for a particular
// JobID. The requests may not necessarily be completed and the statuses of the
// results will reflect this. It is thread safe and the mutex should be locked
// before reading or writing.
type workStoreEntry struct {
	jobID string
	// The mutex should be locked when accessing WorkResults for a particular jobID.
	mu sync.RWMutex
	// Map of RequestIDs to WorkResults. This is what is actually stored in
	// the DB for a particular JobID.
	results map[string]WorkResult
}

// workStore maps JobIDs to the the current results of their work requests. It
// handles automatically loading and unloading the work results as needed from
// BadgerDB into memory to keep overall memory utilization in check.
//
// TODO: https://github.com/ThinkParQ/bee-remote/issues/7.
// Allow specifying the max size of the in-memory work store.
type workStore struct {
	// This mutex is only used to coordinate exclusive access to the overall
	// entries map that represents the in-memory workStore. Individual entries
	// should be locked before they are accessed/modified. In other words this
	// mutex protects the list of keys in the map, not their values. A write
	// lock should be taken before adding an entry to the map.
	mu      sync.Mutex
	entries map[string]*workStoreEntry
	// We don't do any additional locking around the database since badger
	// already gives us transactional guarantees. Developers should be
	// familiar with what guarantees this provides in the context of Badger:
	// https://dgraph.io/docs/badger/get-started/#transactions
	db *badger.DB
}

// newWorkStore attempts to initialize a workStore backed by a database at dbPath.
// It returns an pointer to the workStore and a function that should be called
// to close the database when the workStore is no longer needed.
func newWorkStore(dbPath string) (*workStore, func(), error) {

	db, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		return nil, nil, err
	}

	return &workStore{
		mu:      sync.Mutex{},
		entries: make(map[string]*workStoreEntry),
		db:      db,
	}, func() { db.Close() }, nil
}

// createAndLockEntry() creates a new entry in the workStore for the provided
// JobID. If the provided JobID already has an entry it returns an error.
// Otherwise it creates an entry, locks it, adds it to the in-memory workStore
// entry map, then returns the pointer to the new entry and a function that must
// be used to commit changes to the database and release the lock when the
// caller is finished using it.
func (s *workStore) createAndLockEntry(jobID string) (*workStoreEntry, func() error, error) {

	// First verify the in-memory workStore and database don't already
	// have an entry for jobID. If they do refuse to create a new entry.
	_, commitAndReleaseEntry, err := s.lockAndLoadEntryFromDB(jobID, false)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, nil, fmt.Errorf("unknown error getting work store entry from the DB for job ID %s: %w", jobID, err)
	} else if err == nil {
		commitAndReleaseEntry() // If we got a valid entry we must release it.
		return nil, nil, fmt.Errorf("work store entry already exists for job ID: %s", jobID)
	}

	// If the entry isn't in the database more than likely we'll be able to create it.
	// First lock the in-memory workStore map so we can add an entry.
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[jobID]; ok {
		// This indicates something else raced with us and was able to create a
		// conflicting entry. For now lets not try to add a bunch of logic to
		// prevent this from ever happening. With the way we use the workStore
		// this shouldn't ever be a problem.
		return nil, nil, fmt.Errorf("work store entry already exists for job ID: %s", jobID)
	}

	newWorkEntry := &workStoreEntry{
		jobID:   jobID,
		mu:      sync.RWMutex{},
		results: make(map[string]WorkResult),
	}

	// Lock the new work results entry before adding to the workStore.
	// That way another goroutine can't race and get it.
	newWorkEntry.mu.Lock()
	s.entries[jobID] = newWorkEntry

	return newWorkEntry, s.commitAndReleaseEntry(newWorkEntry), nil
}

// getAndLockEntry is used to get access to the workStore entry for JobID. It
// first checks if the entry is already loaded in the in-memory entries map and
// automatically attempts to load it from the database if needed. It returns a
// pointer to the entry and a function that must be used to release it when
// access is no longer required. If the entry doesn't exist a badger.ErrKeyNotFound
// error will be returned.
func (s *workStore) getAndLockEntry(jobID string) (*workStoreEntry, func() error, error) {

	// First see if there if there is already an entry in the workStore for this jobID:
	entry, ok := s.entries[jobID]
	if !ok {
		// If not see if we can load it from the database:
		entry, commitAndReleaseEntry, err := s.lockAndLoadEntryFromDB(jobID, false)
		if err != nil {
			return nil, nil, fmt.Errorf("work store entry not found for job ID %s and unable to load from the database: %w", jobID, err)
		}
		return entry, commitAndReleaseEntry, nil
	}

	entry.mu.Lock()
	return entry, s.commitAndReleaseEntry(entry), nil
}

// lockAndLoadEntryFromDB attempts to get an entry from the DB and add it to the
// in-memory entries map. If there is already an entry for the specified jobID
// it will always return an error unless overwrite is true. If it is able to add
// an entry to the database it locks the entry before adding it then returns a
// pointer to the entry and a function that should be called to commit the entry
// to the database and release the lock. This ensures the caller is always
// granted exclusive access to the newly loaded entry. If the entry is not found
// in the database then an badger.ErrKeyNotFound error will be returned.
func (s *workStore) lockAndLoadEntryFromDB(jobID string, overwrite bool) (*workStoreEntry, func() error, error) {

	// First lock the workStore then double check the entry doesn't already
	// exists so we don't accidentally overwrite an existing entry unless the
	// caller intended. We'll need the lock anyway before adding an entry.
	s.mu.Lock()
	_, ok := s.entries[jobID]
	if ok && !overwrite {
		s.mu.Unlock()
		return nil, nil, fmt.Errorf("unable to load job ID %s from DB (in-memory entry already exists and overwrite is false)", jobID)
	}
	newEntry := &workStoreEntry{
		jobID: jobID,
		mu:    sync.RWMutex{},
	}
	newEntry.mu.Lock()
	s.entries[jobID] = newEntry
	// Safe to unlock here because we now have an exclusive lock on this specific entry.
	// Reading from the DB could be expensive, we don't want to leave the mutex locked.
	s.mu.Unlock()

	var results = make(map[string]WorkResult)

	if err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(jobID))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			dec := gob.NewDecoder(bytes.NewReader(val))
			err := dec.Decode(&results)
			return err
		})
		return err
	}); err != nil {
		// TODO: If we can't load the results from the database we shouldn't
		// leave an invalid entry in the map. The caller may be able to try
		// again later but would have to know to set overwrite=true which is
		// tricky to coordinate. However right now we don't know if another
		// goroutine is waiting on the lock for this entry, and if we delete it
		// we could panic. So we need to add a WaitGroup to the entries where
		// goroutines waiting on a lock are added so we can coordinate this and
		// garbage collection.
		//
		// s.mu.Lock()
		// delete(s.entries, jobID)
		// s.mu.Unlock()
		return nil, nil, err
	}

	newEntry.results = results
	return newEntry, s.commitAndReleaseEntry(newEntry), nil
}

// commitAndReleaseEntry is used to commit the work results of an already locked
// in-memory entry to the database then release the lock on the entry.
func (s *workStore) commitAndReleaseEntry(entry *workStoreEntry) func() error {

	return func() error {
		defer entry.mu.Unlock()

		var entryBuf bytes.Buffer
		enc := gob.NewEncoder(&entryBuf)

		if err := enc.Encode(entry.results); err != nil {
			return fmt.Errorf("unable to encode work results for Job ID: %w", err)
		}

		if err := s.db.Update(func(txn *badger.Txn) error {
			err := txn.Set([]byte(entry.jobID), entryBuf.Bytes())
			return err
		}); err != nil {
			return fmt.Errorf("unable to update database with work results for Job ID %s: %w", entry.jobID, err)
		}

		return nil
	}
}

// getResults accepts a pointer to a workStoreEntry and returns the
// JobResults expected by JobMgr. We intentionally don't implement getResults as
// a method on the workStore or workResults to avoid lock contention.
func getResults(entry *workStoreEntry) JobResult {

	results := make([]WorkResult, len(entry.results))
	for _, r := range entry.results {
		results = append(results, r)
	}

	jobResults := JobResult{
		JobID:       entry.jobID,
		WorkResults: results,
	}
	return jobResults
}
