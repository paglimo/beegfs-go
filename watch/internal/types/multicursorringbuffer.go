package types

import (
	"fmt"
	"sync"

	pb "git.beegfs.io/beeflex/bee-watch/api/proto/v1"
)

const (
	// The multi-cursor ring buffer garbage collection frequency determines after many new events the Push function checks
	// for and purges events that no longer referenced by any SubscriberCursors by moving forward the start of the buffer.
	// This is to avoid running out of capacity in the ring buffer, which would mean for each Push we have to lock
	// subscriber cursors in case they need to be moved ahead to purge old events.
	// By only requiring this expensive GC to be performed periodically, we can amortize the cost over a large number of Pushes.
	// This only provides a benefit if all subscriber cursors are able to stay ahead of the start of the buffer.
	// Otherwise we have to fall back on garbage collecting the oldest event with every Push.
	mcrbGCFrequency = 1000
)

// MultiCursorRingBuffer is an optimized single writer multi-reader data structure.
// It achieves this optimization by only requiring SubscriberCursors to be lost during garbage collection.
// Otherwise events can be added to the buffer and subscribers can get/ack those events with no lock contention.
type MultiCursorRingBuffer struct {
	// The buffer is only written to by whoever has locked MultiCursorRingBuffer.mutex.
	// The buffer may be read by SubscriberCursors that have locked their respective mutexes.
	// The buffer size is fixed when the buffer is initialized and should never change.
	// Multiple functions rely on len(buf) to calculate when indices should "rollover" (len() is fast = O(1)).
	buffer []*pb.Event
	// Start represents the oldest event in the buffer.
	start int
	// TODO: Do we need a startMutex?
	// startMutex sync.RWMutex
	// End represents the location where the next (newest) event will be written.
	end int
	// cursors is a map of subscriberIDs to SubscriberCursors.
	// TODO: If this is slow consider using https://pkg.go.dev/sync#Map
	// We may also need to add locking around adding and removing cursors.
	cursors map[int]*SubscriberCursor
	// The mutex is used when accessing direct fields of MultiCursorRingBuffer.
	mutex sync.RWMutex
}

type SubscriberCursor struct {
	// sendCursor is the location of the next event to send.
	sendCursor int
	// ackCursor is the location of the next unacknowledged event.
	ackCursor int
	// mutex is used when accessing the sendCursor or ackCursor.
	mutex sync.RWMutex
}

// Returns an ring buffer used to store pointers to events.
// The usable size of the buffer (number of pointers that can be stored in total) is based on size.
// The actual capacity as reflected in memory use is size+1 due to how the buffer distinguishes between an empty and full buffer.
func NewMultiCursorRingBuffer(size int) *MultiCursorRingBuffer {
	return &MultiCursorRingBuffer{
		start:   0,
		end:     0,
		buffer:  make([]*pb.Event, size+1),
		cursors: make(map[int]*SubscriberCursor),
	}
}

// Add cursor accepts a subscriberID and adds a new cursor for that subscriber to the ring buffer.
// It does not return an error if a cursor already exists for the provided subscriberID.
// It is idempotent and multiple calls can be used to definitively confirm a subscriber was added.
func (b *MultiCursorRingBuffer) AddCursor(subscriberID int) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	_, ok := b.cursors[subscriberID]
	if !ok {
		b.cursors[subscriberID] = &SubscriberCursor{
			sendCursor: b.start,
			ackCursor:  b.start,
		}
	}
}

// Remove cursor accepts a subscriberID and removes the cursor for that subscriber (if it exists).
// It does not return an error if the cursor does not already exist.
// It is idempotent and multiple calls can be used to definitively verify a subscriber was deleted.
func (b *MultiCursorRingBuffer) RemoveCursor(subscriberID int) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	delete(b.cursors, subscriberID)
}

// Push adds a new event to the ring buffer.
// It also periodically removes old events from the ring buffer at a fixed interval determined by mcrbGCFrequency.
// At any time if the capacity is exceeded (start == end) the oldest event is overwritten.
// If needed it will advance any sendCursor or ackCursor still pointing at the oldest event to the next oldest event.
func (b *MultiCursorRingBuffer) Push(event *pb.Event) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.buffer[b.end] = event
	b.end = (b.end + 1) % len(b.buffer)

	// If we wrapped around to the start of the buffer we need to free up space:
	if b.start == b.end {
		b.collectGarbage()
	}

	// TODO: Always run collectGarbage based on mcrbGCFrequency.

}

// collectGarbage frees up space in the ring buffer dropping the oldest events first.
// It first examines the SubscriberCursors to determine the oldest unacknowledged event.
// It always frees at least one slot in the buffer.
// If possible it will free up multiple slots at once to optimize performance.
// It SHOULD ONLY be called from Push() as it expects the mutex to already be locked.
func (b *MultiCursorRingBuffer) collectGarbage() {

	// First lets get the position of the oldest ack:
	oldestAckPosition := -1
	currentStart := b.start
	end := b.end

	for _, c := range b.cursors {
		// Lock individual cursors:
		c.mutex.Lock()
		defer c.mutex.Unlock()

		if oldestAckPosition == -1 {
			oldestAckPosition = c.ackCursor
		} else {
			// If the oldest ack is greater or equal to start:
			if oldestAckPosition >= currentStart {
				// Then the oldest events are anything larger than the oldestAckPosition:
				if c.ackCursor > oldestAckPosition {
					oldestAckPosition = c.ackCursor
				} else if c.ackCursor < currentStart {
					// Or anything less than start:
					oldestAckPosition = c.ackCursor
				}
			} else { // Otherwise if the oldest ack is less than start:
				if c.ackCursor < currentStart && c.ackCursor > oldestAckPosition {
					// Then the oldest events are less than start, but greater than oldest ack:
					oldestAckPosition = c.ackCursor
				}
			}
		}
	}

	// We'll always clear the oldest event in the buffer:
	b.buffer[currentStart] = nil
	b.start++

	// If the oldest acknowledged event was already at the old start of the buffer we shouldn't clear more events.
	// Otherwise if the oldest acknowledged event is still -1 there are no subscribers configured yet, so lets keep as many events as possible for now and still only clear the oldest event.
	if oldestAckPosition == currentStart || oldestAckPosition == -1 {
		// Advance any ack or send cursors that were pointing at the oldestAckPosition to the new start:
		for _, c := range b.cursors {
			if c.ackCursor == oldestAckPosition {
				c.ackCursor = b.start
			}

			if c.sendCursor == oldestAckPosition {
				c.sendCursor = b.start
			}
		}
		return
	}

	// Otherwise lets try and free up as much space as we can:
	b.start++
	for {
		// start==end if the buffer is empty. Otherwise keep clearing to the oldest ack position:
		if currentStart == end || currentStart == oldestAckPosition {
			break
		}
		b.buffer[currentStart] = nil
		currentStart = (currentStart + 1) % len(b.buffer)
	}

	// Move start to point at the oldest unacknowledged event.
	b.start = currentStart
}

// GetEvent() returns the next event for the provided subscriberID and moves the sendCursor forward by one.
// * If there is no more events it will return nil.
// * If the subscriber doesn't exist it returns an error.
// For performance reasons it does not lock the ring buffer and looks at buffer[end] to determine the last event.
// It checks if the buffer it is pointing at is nil, and if so treats that as the end of the buffer and doesn't advance the cursor.
// This does mean there are corner cases where it could return nil when an event was just added to the buffer.
// Thus the caller should periodically call GetEvent() or rely on another notification mechanism to determine how many events to get.
func (b *MultiCursorRingBuffer) GetEvent(subscriberID int) (*pb.Event, error) {
	c, ok := b.cursors[subscriberID]
	if !ok {
		return nil, fmt.Errorf("the specified subscriber ID doesn't exist: %d", subscriberID)
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	event := b.buffer[c.sendCursor]

	// Advance the send cursor unless the buffer is empty:
	if event != nil {
		c.sendCursor = (c.sendCursor + 1) % len(b.buffer)
	}

	return event, nil
}

// ResetCursor() moves the sendCursor for a subscriberID back to the ackCursor.
// This would need to happen if the subscriber disconnected for some reason,
// and failed to ack one or more events so we aren't sure if the events were actually sent.
// If the subscriber doesn't exist it returns an error.
func (b *MultiCursorRingBuffer) ResetSendCursor(subscriberID int) error {
	c, ok := b.cursors[subscriberID]
	if !ok {
		return fmt.Errorf("the specified subscriber ID doesn't exist: %d", subscriberID)
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.sendCursor = c.ackCursor

	return nil
}

// AckEvent moves the ackCursor for a subscriberID forward to the next unacknowledged seqID.
// It works as follows:
// (1) If the seqID of the current ackCursor is greater then the provided seqID do nothing.
// (2) If the seqID at the current ackCursor matches the provided seqID increase the ackCursor by one.
// (3) If the provided seqID is between the ackCursor.SeqID and sendCursor.SeqID, try to calculate the expected location of seqID.
//   - If found increase the ackCursor by one.
//
// (4) If unable to calculate the expected location, fall back on a binary search between the ackCursor and sendCursor.
//   - If found increase the ackCursor by one.
//   - Otherwise point the ackCursor at the next highest seqID after the acknowledged seqID.
//
// If the seqID is greater than or equal to the seqID at the sendCursor it returns an error.
// If the provided subscriberID doesn't exist it returns an error.
// If the ackCursor points at a nil event it returns an error.
func (b *MultiCursorRingBuffer) AckEvent(subscriberID int, seqID uint64) error {
	c, ok := b.cursors[subscriberID]
	if !ok {
		return fmt.Errorf("the specified subscriber ID doesn't exist: %d", subscriberID)
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// This should only happen if the subscriber calls AckEvent before any events were actually sent (and the buffer is empty).
	if b.buffer[c.ackCursor] == nil {
		return fmt.Errorf("subscriber tried to acknowledge an event but the next event to acknowledge is nil: is the buffer empty? (subscriber: %d, seqID %d)", subscriberID, seqID)
	}

	// We shouldn't hit this unless there is a bug somewhere.
	if b.buffer[c.sendCursor] == nil {
		return fmt.Errorf("subscriber tried to acknowledge an event but the next expected event is nil: is the buffer empty? (subscriber: %d, seqID %d)", subscriberID, seqID)
	}

	// TODO: Do we actually need some of these error checks? Consider removing once BeeWatch is in a more stable state.
	// Otherwise we should probably add more checking to account for "holes" in the buffer.
	// For example if there are events that are nil surrounded by valid events we would panic.

	if b.buffer[c.ackCursor].SeqId > seqID {
		// Subscriber either sent a double ack or we dropped the event the subscriber tried to acknowledge.
		// Either way don't move the ack cursor and don't return an error as there is nothing more we can do.
		return nil
	} else if b.buffer[c.ackCursor].SeqId == seqID {
		// Subscriber acknowledged the next expected event.
		// Just increment the ack cursor by one.
		c.ackCursor = (c.ackCursor + 1) % len(b.buffer)
		return nil
	} else if seqID > b.buffer[c.ackCursor].SeqId && seqID < b.buffer[c.sendCursor].SeqId {
		// Otherwise it is possible the subscriber is not acknowledging each event.
		// Since events should always be in order in the buffer,
		// and our seqID falls between the ackCursor and the sendCursor,
		// it is likely we can calculate the location of this seqID in the buffer.
		// Try adding the difference between the acknowledged seqID and the last acknowledged sequence ID (ackCursorSeqID).
		// We then add this to the current location of the ackCursor wrapping it around the end of the buffer if needed.
		expectedLocation := (uint64(c.ackCursor) + (seqID - b.buffer[c.ackCursor].SeqId)) % uint64(len(b.buffer))
		expectedLocSeqID := b.buffer[expectedLocation].SeqId
		if expectedLocSeqID == seqID {
			c.ackCursor = int(expectedLocation)
			return nil
		}
		// If the event wasn't where we expected, probably there was a dropped event.
		// We'll fall back on our slower mechanism to find it.
	}

	if seqID < b.buffer[c.sendCursor].SeqId {
		// If we can't calculate the exact position of the acknowledged event, probably there is a dropped event somewhere.
		// Perhaps our internal buffer overflowed or the meta dropped an event.
		// We'll fall back on binary search to point the ackCursor at the next closet event we expect to be acknowledged.
		// We can only do this as long as our seqID is less than the sendCursorSeqID.
		// This should be the case as long as the subscriber is not acknowledging events before we send them.

		ackdEventIndex, foundExact := b.searchIndexOfSeqID(c.ackCursor, c.sendCursor-1, seqID)

		// This shouldn't happen unless the containing if statement was changed or there is a bug in searchIndexOfSeqID().
		// We should only ever attempt the search if the targetSeqID is less than the sendCursorSeqID.
		// The search should only return -1 if all numbers between the ackCursor and sendCursor-1 are less than targetSeqID.
		if ackdEventIndex == -1 {
			return fmt.Errorf("unable to find the specified seqID or the next closest seqID (subscriber: %d, seqID %d)", subscriberID, seqID)
		}

		if foundExact {
			// The ackCursor should point at the next event we expect to acknowledge.
			// If we found the exact event that was acknowledged this will be the next index.
			c.ackCursor = (ackdEventIndex + 1) % len(b.buffer)
		} else {
			// Otherwise if we didn't find the exact event, what was returned is the next seqID that should be acknowledged.
			c.ackCursor = (ackdEventIndex) % len(b.buffer)
		}

		return nil

	} else if seqID >= b.buffer[c.sendCursor].SeqId {
		return fmt.Errorf("subscriber tried to acknowledge an event that wasn't sent yet (subscriber: %d, seqID %d)", subscriberID, seqID)
	}

	// In theory we should never hit this vague error..
	return fmt.Errorf("unable to acknowledge event (subscriber: %d, seqID %d)", subscriberID, seqID)
}

// searchIndexOfSeqID is a ring buffer aware binary search implementation.
// It looks for targetSeqID in the buffer between startIndex and endIndex.
// If targetSeqID is found the index is returned along with true.
// Otherwise it returns the next index (higher) with the lowest seqID and false.
// If the targetSeqID is greater than the seqID at endIndex it returns -1, false.
func (b *MultiCursorRingBuffer) searchIndexOfSeqID(startIndex int, endIndex int, targetSeqID uint64) (int, bool) {

	size := len(b.buffer)
	low := 0
	high := (endIndex - startIndex + size) % size
	nextLowest := -1                      // If all seqIDs are less than targetSeqID we'll return -1.
	var nextLowestVal uint64 = ^uint64(0) // Get the maximum uint64 value by using the bitwise compliment operator on 0.

	// We use low and high as the logical window for our search:
	for low <= high {
		// Mid represents the middle index in the logical view of the buffer:
		mid := (low + (high-low)/2)
		// realMid converts the logical index to the actual index taking into consideration the ring buffer wraps around:
		realMid := (startIndex + mid) % size

		if b.buffer[realMid].SeqId == targetSeqID {
			return realMid, true
		} else if b.buffer[realMid].SeqId < targetSeqID {
			low = mid + 1
		} else {
			high = mid - 1
			if nextLowestVal > b.buffer[realMid].SeqId {
				nextLowest = realMid
				nextLowestVal = b.buffer[realMid].SeqId
			}
		}
	}

	return nextLowest, false
}
