package worker

import (
	"sync"

	beegfs "github.com/thinkparq/protobuf/beegfs/go"
)

// Supported worker nodes are organized into pools based on their NodeType. In
// addition to implementing the Worker interface, expand the list of constants
// below when adding new worker node types. This will allow a pool for the new
// worker type to automatically be created whenever new workers of that type are
// configured.
type NodeType string

const (
	BeeSync NodeType = "beesync"
)

// All worker nodes must implement the Interface interface.
type Interface interface {
	Connect() error
	Send(WorkRequest) error
	Recv() <-chan *beegfs.WorkResponse
	Disconnect() error
	GetNodeType() NodeType
}

// state should not be used directly.
// It should be set/inspected using the exported thread safe State methods.
type state string

const (
	DISCONNECTED  state = "disconnected"
	CONNECTING    state = "connecting"
	CONNECTED     state = "connected"
	DISCONNECTING state = "disconnecting"
)

// The GetState method provided by State are used to communicate the state of a worker to the outside world.
// External readers SHOULD NOT inspect the state directly, but instead use the thread safe GetState() method.
// The Handler.Handle() method is the ONLY place where WorkerStates should change.
// All internal handler methods and subscriber methods such as connect(), send(), disconnect(), etc. should not affect the state.
type State struct {
	// state should only be accessed through the GetStateStatus() and SetStateStatus() methods to ensure thread safety.
	state state
	mutex sync.RWMutex
}

// GetState is a thread safe mechanism to get the current state of a worker.
func (s *State) GetState() state {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

// SetState is a thread safe mechanism to set the current state of a worker.
func (s *State) SetState(newState state) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.state = newState
}

// Worker encapsulates the configuration and functionality implemented by
// all workers. It uses an Interface to abstract implementation details for
// a particular worker type to allow a common handler.
type Worker struct {
	Config
	State
	Interface
}

// ComparableWorker is a "comparable" view of the Worker struct used for testing.
// When Worker is updated it should also be updated with any fields that are a comparable type.
// Notably the interface is omitted and each Worker should implement its own comparable type.
type ComparableWorker struct {
	Config
	State
}

// WorkRequest represents an interface for work requests that can be processed
// by different types of worker nodes.
type WorkRequest interface {
	// getJobID() returns the job id.
	getJobID() string
	// getRequestID() returns the request ID. Note request IDs are only
	// guaranteed to be unique for a particular job.
	getRequestID() string
	// getStatus() returns the status of the request.
	getStatus() beegfs.RequestStatus
	// getNodeType() is implemented by BaseWR.
	getNodeType() NodeType
}
