package worker

import (
	"fmt"
	"sync"

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
	mu      sync.RWMutex
	entries map[string]*workStoreEntry
}

func newWorkStore() *workStore {
	return &workStore{
		mu:      sync.RWMutex{},
		entries: make(map[string]*workStoreEntry),
	}
}

// createAndLock() creates a new workStoreEntry for the provided JobID.
// If the provided JobID already has an entry it returns an error.
// Otherwise it creates a workResults entry, locks it, adds it to the entry
// map, then returns the pointer to the new entry and a function that must
// be used to release it when access is no longer required,
func (s *workStore) createAndLock(jobID string) (*workStoreEntry, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[jobID]; ok {
		return nil, nil, fmt.Errorf("cannot create a new work store entry for existing job: %s", jobID)
	}
	workResults := &workStoreEntry{
		jobID:   jobID,
		mu:      sync.RWMutex{},
		results: make(map[string]WorkResult),
	}

	// Lock the new work results entry before adding to the workStore.
	// That way another goroutine can't race and get it.
	workResults.mu.Lock()
	s.entries[jobID] = workResults

	return workResults, func() { workResults.mu.Unlock() }, nil
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
