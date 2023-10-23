package worker

import (
	"context"
	"math/rand"
	"sync"
	"time"

	beegfs "github.com/thinkparq/protobuf/beegfs/go"
	"go.uber.org/zap"
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

// All concrete worker node types must implement the Worker interface.
type Worker interface {
	Connect() (retry bool, err error)
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

// setState is a thread safe mechanism to set the current state of a worker
// although with the current us it doesn't need to be. It should only be called
// from within Handle().
func (s *State) setState(newState state) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.state = newState
}

// Node encapsulates the configuration and functionality implemented by all
// types of worker nodes. It uses an Interface to abstract implementation
// details for a particular worker type to allow a common handler.
type Node struct {
	Config
	State
	// Worker should be set equal to a concrete type that fulfills the Worker
	// interface. This is what determines what type of Node we're working with.
	worker Worker
	ctx    context.Context
	cancel context.CancelFunc
	log    *zap.Logger
	// The mutex serves two purposes: (1) guarantee only one Handle() methods
	// for each node at a time. (2) guarantee when dynamic configuration updates
	// happen the old node handler has finished shutting down before we swap out
	// the node or delete it.
	mu sync.Mutex
	// Incoming requests that should be sent to the node using Send() are queued here.
	WorkQueue chan WorkRequest
	// All work request responses are sent to this channel. This includes responses
	// from the external node (via Recv()) or responses to local errors from Send()
	// or if a WorkRequest was assigned to a node while it was disconnected.
	workResponses chan<- *beegfs.WorkResponse
}

// ComparableNode is a "comparable" view of the Node struct used for testing.
// When Node is updated it should also be updated with any fields that are a comparable type.
// Notably the interface is omitted and each worker should implement its own comparable type.
type ComparableNode struct {
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
	// setStatus() sets the request status and message.
	setStatus(beegfs.RequestStatus_Status, string)
	// getNodeType() is implemented by BaseWR.
	getNodeType() NodeType
}

// WorkResult carries status and node assignment for a particular WorkRequest.
// It is setup so it can be copied when needed, either in JobResults or when
// saving WorkResults to disk. The requests may not necessarily be completed and
// the statuses of the results will reflect this.
type WorkResult struct {
	RequestID string
	Status    beegfs.RequestStatus_Status
	Message   string
	// Assigned to indicates the node running this work request or "" if it is unassigned.
	AssignedTo string
}

// Handles the connection with a particular worker node. It determines the state
// of the worker node (i.e., connected, disconnected) based on external and
// internal factors. It is the only place that should update the state of the node.
func (n *Node) Handle(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	n.mu.Lock()
	defer n.mu.Unlock()

	for {
		select {
		case <-n.ctx.Done():
			n.log.Debug("successfully shutdown connection to worker node")
			return
		default:

			// We look at the result of the last loop to tell us what needs to
			// happen next. If we're disconnected we should connect. If we're
			// connected we should start handling the connection. Otherwise we
			// presume we need to disconnect for some reason.
			if state := n.GetState(); state == DISCONNECTED {
				n.setState(CONNECTING)
				if n.connectLoop() {
					n.setState(CONNECTED)

					// We need to start listening for responses from the node
					// before we do anything. Some nodes may block on receiving
					// new work requests until they can send responses. Some
					// nodes may also tell us the last work request they
					// successfully received so we can avoid sending duplicate
					// work requests.
					doneReceiving, cancelReceive := n.receiveLoop()

					// Start sending events to this subscriber:
					doneSending, cancelSend := n.sendLoop()

					// If either the receive or send goroutines are done, we
					// should fully disconnect:
					select {
					case <-doneReceiving:
						cancelSend()
						<-doneSending
					case <-doneSending:
						cancelReceive()
						<-doneReceiving
					case <-n.ctx.Done():
						cancelSend()
						<-doneSending
						cancelReceive()
						<-doneReceiving
					}
					n.setState(DISCONNECTING)
				}
			}
			// If the connection was lost for any reason, we should first
			// disconnect before we reconnect or shutdown:
			if n.doDisconnect() {
				n.setState(DISCONNECTED)
			}
		}
	}
}

func (n *Node) doDisconnect() bool {
	n.log.Info("disconnecting worker node")
	err := n.worker.Disconnect()
	if err != nil {
		n.log.Error("encountered one or more errors disconnecting worker node (ignoring)", zap.Error(err))
		return false
	}
	n.log.Info("disconnected worker node")
	return true
}

// connectLoop() attempts to connect to a worker node. If the node is not ready
// or there is an error it will attempt to reconnect with an exponential
// backoff. If it returns false there was an unrecoverable error and the caller
// should first call doDisconnect() before reconnecting.
func (n *Node) connectLoop() bool {
	n.log.Info("connecting to worker node")
	var reconnectBackOff float64 = 1
	for {
		select {
		case <-n.ctx.Done():
			n.log.Info("not attempting to connect to worker node because the handler is shutting down")
			return false
		case <-time.After(time.Second * time.Duration(reconnectBackOff)): // We use this instead of time.Ticker so we can change the duration.
			retry, err := n.worker.Connect()
			if err != nil {
				if !retry {
					n.log.Error("unable to connect to worker node (unable to retry)", zap.Error(err))
					return false
				}

				// We'll retry to connect with an exponential back off. We'll add some jitter to avoid load spikes.
				reconnectBackOff *= 2 + rand.Float64()
				if reconnectBackOff > float64(n.MaxReconnectBackOff) {
					reconnectBackOff = float64(n.MaxReconnectBackOff) - rand.Float64()
				}

				n.log.Warn("unable to connect to worker node (retrying)", zap.Error(err), zap.Any("retryInSeconds", reconnectBackOff))
				continue
			}

			n.log.Info("connected to worker node")
			return true
		}
	}
}

func (n *Node) receiveLoop() (<-chan struct{}, context.CancelFunc) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	recvStream := n.worker.Recv()

	go func() {
		defer close(done)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				n.log.Debug("no longer listening for responses because the handler is shutting down")
				return
			case response, ok := <-recvStream:
				if !ok {
					n.log.Info("no longer listening for responses because the worker node disconnected")
					return
				}
				n.log.Debug("received response from worker node", zap.Any("response", response))
				n.workResponses <- response
			}
		}
	}()
	return done, cancel
}

// sendLoop() handles sending work requests to the node. It will do this until
// the connection breaks for any reason (gracefully or otherwise). Once it
// returns the connection must be disconnected and reconnected before sendLoop()
// is called again. It does not return an error because the caller should react
// the same in all scenarios.
func (n *Node) sendLoop() (<-chan struct{}, context.CancelFunc) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	n.log.Debug("establishing work request stream")

	go func() {
		defer close(done)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				n.log.Debug("no longer sending work requests because the handler is shutting down")
				return
			case wr := <-n.WorkQueue:
				if err := n.worker.Send(wr); err != nil {
					n.log.Error("unable to send work request", zap.Error(err))
					workResponse := &beegfs.WorkResponse{
						JobId:     wr.getJobID(),
						RequestId: wr.getRequestID(),
						Status: &beegfs.RequestStatus{
							Status:  beegfs.RequestStatus_UNASSIGNED,
							Message: err.Error(),
						},
					}
					n.workResponses <- workResponse
				}
				// TODO: The send doesn't guarantee the worker node will
				// actually receive and get a chance to save the request to its
				// local DB. Because BeeRemote doesn't ever follow up on
				// outstanding WRs, this means we could end up with orphaned WRs
				// that would require user intervention to recover. Figure out
				// how we want to handle this.
			}
		}
	}()
	return done, cancel
}

func (n *Node) Stop() {
	n.log.Info("shutting down connection to worker node")
	n.cancel()
}
