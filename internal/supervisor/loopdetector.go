// Package supervisor manages child runner execution, loop detection, and process lifecycles.
package supervisor

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// LoopDetectionResult reports whether a repetitive execution pattern was detected.
type LoopDetectionResult struct {
	IsLoop      bool
	Reason      string
	RepeatCount int
	Action      string
}

// ActionRecord captures one execution step for loop analysis.
type ActionRecord struct {
	ActionHash string
	Action     string
	OutputHash string
	Timestamp  time.Time
}

// LoopDetector monitors agent actions and detects repetitive loops or failure thrashing.
type LoopDetector struct {
	mu                   sync.Mutex
	history              []ActionRecord
	stateHashes          []string
	maxHistory           int
	consecutiveThreshold int
	cycleThreshold       int
}

// NewLoopDetector creates a loop detector with enterprise thresholds.
func NewLoopDetector(maxHistory, consecutiveThreshold, cycleThreshold int) *LoopDetector {
	if maxHistory <= 0 {
		maxHistory = 20
	}
	if consecutiveThreshold <= 0 {
		consecutiveThreshold = 3
	}
	if cycleThreshold <= 0 {
		cycleThreshold = 2
	}
	return &LoopDetector{
		maxHistory:           maxHistory,
		consecutiveThreshold: consecutiveThreshold,
		cycleThreshold:       cycleThreshold,
	}
}

// Record ingests an action and output, returning whether a loop was detected.
func (d *LoopDetector) Record(action, output string) LoopDetectionResult {
	d.mu.Lock()
	defer d.mu.Unlock()

	action = strings.TrimSpace(action)
	actHash := hashString(action)
	outHash := hashString(strings.TrimSpace(output))

	rec := ActionRecord{
		ActionHash: actHash,
		Action:     action,
		OutputHash: outHash,
		Timestamp:  time.Now().UTC(),
	}

	d.history = append(d.history, rec)
	if len(d.history) > d.maxHistory {
		d.history = d.history[len(d.history)-d.maxHistory:]
	}

	// 1. Check for consecutive identical actions with static output (stagnant loop)
	repeats := 0
	identicalOutput := 0
	for i := len(d.history) - 1; i >= 0; i-- {
		if d.history[i].ActionHash == actHash {
			repeats++
			if d.history[i].OutputHash == outHash {
				identicalOutput++
			}
		} else {
			break
		}
	}

	if repeats >= d.consecutiveThreshold {
		if identicalOutput >= d.consecutiveThreshold {
			return LoopDetectionResult{
				IsLoop:      true,
				Reason:      fmt.Sprintf("detected %d consecutive identical actions with static output (stagnant loop)", repeats),
				RepeatCount: repeats,
				Action:      action,
			}
		}
		if repeats >= d.consecutiveThreshold+2 {
			return LoopDetectionResult{
				IsLoop:      true,
				Reason:      fmt.Sprintf("detected %d consecutive identical actions despite output variations", repeats),
				RepeatCount: repeats,
				Action:      action,
			}
		}
	}

	// 2. Check for identical error/output thrashing across different actions
	if outHash != "" && outHash != hashString("") {
		sameOutputCount := 0
		for i := len(d.history) - 1; i >= 0; i-- {
			if d.history[i].OutputHash == outHash {
				sameOutputCount++
			} else {
				break
			}
		}
		if sameOutputCount >= d.consecutiveThreshold {
			return LoopDetectionResult{
				IsLoop:      true,
				Reason:      fmt.Sprintf("detected %d consecutive identical outputs across actions (failure thrashing)", sameOutputCount),
				RepeatCount: sameOutputCount,
				Action:      action,
			}
		}
	}

	// 3. Check for 2-step alternating cycles (A -> B -> A -> B ...)
	if len(d.history) >= 4 {
		n := len(d.history)
		a1 := d.history[n-1].ActionHash
		b1 := d.history[n-2].ActionHash
		a2 := d.history[n-3].ActionHash
		b2 := d.history[n-4].ActionHash

		if a1 == a2 && b1 == b2 && a1 != b1 {
			outA1 := d.history[n-1].OutputHash
			outA2 := d.history[n-3].OutputHash
			outB1 := d.history[n-2].OutputHash
			outB2 := d.history[n-4].OutputHash
			if outA1 == outA2 && outB1 == outB2 {
				return LoopDetectionResult{
					IsLoop:      true,
					Reason:      "detected 2-step cyclic alternating loop thrashing with identical state cycle",
					RepeatCount: 2,
					Action:      action,
				}
			}
			return LoopDetectionResult{
				IsLoop:      true,
				Reason:      "detected 2-step cyclic alternating loop thrashing",
				RepeatCount: 2,
				Action:      action,
			}
		}
	}

	return LoopDetectionResult{
		IsLoop:      false,
		RepeatCount: repeats,
		Action:      action,
	}
}

// RecordState records a working-tree AST/diff state hash into the ring buffer.
// If the exact same working-tree state hash appears twice in a single trajectory,
// it triggers an immediate circuit break to stop oscillating fix loops.
func (d *LoopDetector) RecordState(stateHash string) LoopDetectionResult {
	d.mu.Lock()
	defer d.mu.Unlock()

	stateHash = strings.TrimSpace(stateHash)
	if stateHash == "" {
		return LoopDetectionResult{IsLoop: false}
	}

	for _, existing := range d.stateHashes {
		if existing == stateHash {
			return LoopDetectionResult{
				IsLoop:      true,
				Reason:      fmt.Sprintf("detected oscillating working-tree state loop (working tree reverted to previously seen state %s)", stateHash[:min(8, len(stateHash))]),
				RepeatCount: 2,
				Action:      stateHash,
			}
		}
	}

	d.stateHashes = append(d.stateHashes, stateHash)
	if len(d.stateHashes) > d.maxHistory {
		d.stateHashes = d.stateHashes[len(d.stateHashes)-d.maxHistory:]
	}

	return LoopDetectionResult{IsLoop: false}
}

// RecordWithState evaluates both the action/output sequence and the working-tree state hash.
func (d *LoopDetector) RecordWithState(action, output, stateHash string) LoopDetectionResult {
	if stateHash != "" {
		stateRes := d.RecordState(stateHash)
		if stateRes.IsLoop {
			return stateRes
		}
	}
	return d.Record(action, output)
}

// ComputeWorkingTreeHash computes a deterministic SHA256 of sorted file paths and their contents.
func ComputeWorkingTreeHash(files map[string][]byte) string {
	if len(files) == 0 {
		return hashString("empty-tree")
	}
	keys := make([]string, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write(files[k])
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// ComputeDiffHash computes a deterministic SHA256 of git diff text.
func ComputeDiffHash(diff string) string {
	clean := strings.TrimSpace(diff)
	return hashString(clean)
}

// Reset clears the execution history and state ring buffer.
func (d *LoopDetector) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.history = nil
	d.stateHashes = nil
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}
