// Package cluster provides a sorted-set API (via Redis ZSETs) on top of a
// group of Redis instances.
package cluster

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"

	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/instrumentation"
	"github.com/soundcloud/roshi/shard"
	"github.com/soundcloud/roshi/vendor/redigo/redis"
)

// Cluster defines methods that efficiently provide ZSET semantics on a
// cluster.
type Cluster interface {
	Inserter
	Selecter
	Deleter
	Scorer
	Scanner
}

// Inserter defines the method to add elements to a sorted set. A key-member's
// score must be larger than the currently stored score for the insert to be
// accepted. A non-nil error indicates only physical problems, not logical.
type Inserter interface {
	Insert(tuples []common.KeyScoreMember) error
}

// Selecter defines the method to retrieve elements from a sorted set.
type Selecter interface {
	Select(keys []string, offset, limit int) <-chan Element
}

// Deleter defines the method to delete elements from a sorted set. A key-
// member's score must be larger than the currently stored score for the delete
// to be accepted. A non-nil error indicates only physical problems, not
// logical.
type Deleter interface {
	Delete(tuples []common.KeyScoreMember) error
}

// Scorer defines the method to retrieve the score of a specific key-member,
// if it's known to the cluster. It also returns whether that key-member is
// inserted (present) or deleted (not present) via the bool return parameter
// (true = inserted).
type Scorer interface {
	Score(key, member string) (float64, bool, error)
}

// Scanner emits all keys in the keyspace over a returned
// channel. When the keys are exhaused, the channel is closed. The
// order in which keys are emitted is unpredictable. Scanning is
// performed one Redis instance at a time in random order of the
// instances. If an instance is down at the time it is tried to be
// scanned, it is skipped (no retries). See also implications of the
// Redis SCAN command.
type Scanner interface {
	Keys() chan string
}

const (
	insertSuffix = "+"
	deleteSuffix = "-"
)

var (
	genericScript = `
		local addKey = KEYS[1] .. 'ADDSUFFIX'
		local remKey = KEYS[1] .. 'REMSUFFIX'

		local maxSize = tonumber(ARGV[3])
		local atCapacity = tonumber(redis.call('ZCARD', addKey)) >= maxSize
		if atCapacity then
			local oldestTs = redis.call('ZRANGE', addKey, 0, 0, 'WITHSCORES')[2]
			if oldestTs and tonumber(ARGV[1]) < tonumber(oldestTs) then
				return -1
			end
		end

		local insertTs = redis.call('ZSCORE', KEYS[1] .. 'INSERTSUFFIX', ARGV[2])
		local deleteTs = redis.call('ZSCORE', KEYS[1] .. 'DELETESUFFIX', ARGV[2])
		if insertTs and tonumber(ARGV[1]) < tonumber(insertTs) then
			return -1
		elseif deleteTs and tonumber(ARGV[1]) < tonumber(deleteTs) then
			return -1
		end

		redis.call('ZREM', remKey, ARGV[2])
		local n = redis.call('ZADD', addKey, ARGV[1], ARGV[2])
		redis.call('ZREMRANGEBYRANK', addKey, 0, -(maxSize+1))
		return n
	`
	insertScript *redis.Script
	deleteScript *redis.Script
)

func init() {
	genericScript = strings.NewReplacer(
		"INSERTSUFFIX", insertSuffix,
		"DELETESUFFIX", deleteSuffix,
	).Replace(genericScript)

	insertScript = redis.NewScript(1, strings.NewReplacer(
		"REMSUFFIX", deleteSuffix, // Insert script does ZREM from deletes key
		"ADDSUFFIX", insertSuffix, // and ZADD to inserts key
	).Replace(genericScript))

	deleteScript = redis.NewScript(1, strings.NewReplacer(
		"REMSUFFIX", insertSuffix, // Delete script does ZREM from inserts key
		"ADDSUFFIX", deleteSuffix, // and ZADD to deletes key
	).Replace(genericScript))
}

// cluster implements the Cluster interface on a concrete Redis cluster.
type cluster struct {
	shards          *shard.Shards
	maxSize         int
	instrumentation instrumentation.Instrumentation
}

// New creates and returns a new Cluster backed by a concrete Redis cluster.
// maxSize for each key will be enforced at write time. Instrumentation may be
// nil.
func New(shards *shard.Shards, maxSize int, instr instrumentation.Instrumentation) Cluster {
	if instr == nil {
		instr = instrumentation.NopInstrumentation{}
	}
	return &cluster{
		shards:          shards,
		maxSize:         maxSize,
		instrumentation: instr,
	}
}

// Insert efficiently performs ZADDs for each of the passed tuples.
func (c *cluster) Insert(tuples []common.KeyScoreMember) error {
	// Bucketize
	m := map[int][]common.KeyScoreMember{}
	for _, tuple := range tuples {
		index := c.shards.Index(tuple.Key)
		m[index] = append(m[index], tuple)
	}

	// Scatter
	errChan := make(chan error, len(m))
	for index, tuples := range m {
		go func(index int, tuples []common.KeyScoreMember) {

			errChan <- c.shards.WithIndex(index, func(conn redis.Conn) error {
				return pipelineInsert(conn, tuples, c.maxSize)
			})

		}(index, tuples)
	}

	// Gather
	for _ = range m {
		if err := <-errChan; err != nil {
			return err
		}
	}
	return nil
}

// Select effeciently performs ZREVRANGEs for each of the passed keys using
// the offset and limit for each. It pushes results to the returned chan as
// they become available.
func (c *cluster) Select(keys []string, offset, limit int) <-chan Element {
	out := make(chan Element)
	go func() {
		// Bucketize
		m := map[int][]string{}
		for _, key := range keys {
			index := c.shards.Index(key)
			m[index] = append(m[index], key)
		}

		// Scatter. We need to return an element for all of the input keys, but it
		// can be an error element. Client does the gathering.
		wg := sync.WaitGroup{}
		wg.Add(len(m))
		for index, keys := range m {
			go func(index int, keys []string) {
				defer wg.Done()

				// Make channel sends outside of this function, to
				// minimize our time with the redis.Conn.
				var elements []Element
				var result map[string][]common.KeyScoreMember
				if err := c.shards.WithIndex(index, func(conn redis.Conn) (err error) {
					result, err = pipelineRevRange(conn, keys, offset, limit)
					return
				}); err != nil {
					elements = errorElements(keys, err)
				} else {
					elements = successElements(result)
				}

				for _, element := range elements {
					out <- element
				}
			}(index, keys)
		}
		wg.Wait()

		// Signal that we're done to the client.
		close(out)
	}()
	return out
}

// Delete efficiently performs ZREMs for each of the passed tuples.
func (c *cluster) Delete(tuples []common.KeyScoreMember) error {
	// Bucketize
	m := map[int][]common.KeyScoreMember{}
	for _, tuple := range tuples {
		index := c.shards.Index(tuple.Key)
		m[index] = append(m[index], tuple)
	}

	// Scatter
	errChan := make(chan error, len(m))
	for index, tuples := range m {
		go func(index int, tuples []common.KeyScoreMember) {
			errChan <- c.shards.WithIndex(index, func(conn redis.Conn) error {
				return pipelineDelete(conn, tuples, c.maxSize)
			})

		}(index, tuples)
	}

	// Gather
	for _ = range m {
		if err := <-errChan; err != nil {
			return err
		}
	}
	return nil
}

// Score returns the score of the given key-member combination and
// whether that score has been found with the deletes or the inserts
// key (true: inserts, false: deletes).
func (c *cluster) Score(key, member string) (float64, bool, error) {
	var score float64
	var inserted bool
	err := c.shards.With(key, func(conn redis.Conn) error {
		var err error
		score, inserted, err = pipelineScore(conn, key, member)
		return err
	})
	return score, inserted, err
}

// Keys implements the Scanner interface.
func (c *cluster) Keys() chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		for index := range rand.Perm(c.shards.Size()) {
			cursor := 0
			if err := c.shards.WithIndex(index, func(conn redis.Conn) error {
				values, err := redis.Values(conn.Do(fmt.Sprintf("SCAN %d", cursor)))
				if err != nil {
					return err
				}
				if n := len(values); n != 2 {
					return fmt.Errorf("received %d values from Redis, expected exactly 2", n)
				}
				if cursor, err = redis.Int(values[0], nil); err != nil {
					return err
				}
				keys, err := redis.Strings(values[1], nil)
				if err != nil {
					return err
				}
				for _, key := range keys {
					ch <- key
				}
				return nil
			}); err != nil {
				log.Printf("cluster: during Keys on instance %d: %s", index, err)
				continue // Skip failed instance.
			}
		}
	}()
	return ch
}

func pipelineInsert(conn redis.Conn, tuples []common.KeyScoreMember, maxSize int) error {
	for _, tuple := range tuples {
		if err := insertScript.Send(
			conn,
			tuple.Key,
			tuple.Score,
			tuple.Member,
			maxSize,
		); err != nil {
			return err
		}
	}

	if err := conn.Flush(); err != nil {
		return err
	}

	for _ = range tuples {
		// TODO actually count inserts
		if _, err := conn.Receive(); err != nil {
			return err
		}
	}

	return nil
}

// Element combines a submitted key with its selected score-members. If there
// was an error while selecting a key, the error field will be populated, and
// common.KeyScoreMembers may be empty. TODO rename.
type Element struct {
	Key             string
	KeyScoreMembers []common.KeyScoreMember
	Error           error
}

func errorElements(keys []string, err error) []Element {
	elements := make([]Element, len(keys))
	for i := range keys {
		elements[i] = Element{
			Key:             keys[i],
			KeyScoreMembers: []common.KeyScoreMember{},
			Error:           err,
		}
	}
	return elements
}

func successElements(m map[string][]common.KeyScoreMember) []Element {
	elements := make([]Element, 0, len(m))
	for key, keyScoreMembers := range m {
		elements = append(elements, Element{
			Key:             key,
			KeyScoreMembers: keyScoreMembers,
			Error:           nil,
		})
	}
	return elements
}

func pipelineRevRange(conn redis.Conn, keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
	for _, key := range keys {
		if err := conn.Send(
			"ZREVRANGE",
			key+insertSuffix,
			offset,
			offset+limit-1,
			"WITHSCORES",
		); err != nil {
			return map[string][]common.KeyScoreMember{}, err
		}
	}

	if err := conn.Flush(); err != nil {
		return map[string][]common.KeyScoreMember{}, err
	}

	m := make(map[string][]common.KeyScoreMember, len(keys))
	for _, key := range keys {
		values, err := redis.Values(conn.Receive())
		if err != nil {
			return map[string][]common.KeyScoreMember{}, err
		}

		tuples := make([]common.KeyScoreMember, 0, limit)
		for len(values) > 0 {
			var member string
			var score float64
			if values, err = redis.Scan(values, &member, &score); err != nil {
				return map[string][]common.KeyScoreMember{}, err
			}
			tuples = append(tuples, common.KeyScoreMember{Key: key, Score: score, Member: member})
		}
		m[key] = tuples
	}
	return m, nil
}

func pipelineDelete(conn redis.Conn, tuples []common.KeyScoreMember, maxSize int) error {
	for _, tuple := range tuples {
		if err := deleteScript.Send(
			conn,
			tuple.Key,
			tuple.Score,
			tuple.Member,
			maxSize,
		); err != nil {
			return err
		}
	}

	if err := conn.Flush(); err != nil {
		return err
	}

	for _ = range tuples {
		// TODO actually count deletes
		if _, err := conn.Receive(); err != nil {
			return err
		}
	}

	return nil
}

func pipelineScore(conn redis.Conn, key string, member string) (float64, bool, error) {
	if err := conn.Send("MULTI"); err != nil {
		return 0, false, err
	}
	if err := conn.Send("ZSCORE", key+insertSuffix, member); err != nil {
		return 0, false, err
	}
	if err := conn.Send("ZSCORE", key+deleteSuffix, member); err != nil {
		return 0, false, err
	}
	if err := conn.Send("EXEC"); err != nil {
		return 0, false, err
	}
	if err := conn.Flush(); err != nil {
		return 0, false, err
	}

	// Fast forward through the first three replies. Just make
	// sure they have not resulted in an error. (The actual reply
	// content is boring: "OK", "QUEUED", "QUEUED".)
	for i := 0; i < 3; i++ {
		if _, err := conn.Receive(); err != nil {
			return 0, false, err
		}
	}
	values, err := redis.Values(conn.Receive())
	if err != nil {
		return 0, false, err
	}

	// Some sanity checks.
	if n := len(values); n != 2 {
		return 0, false, fmt.Errorf("received %d values from Redis, expected exactly 2", n)
	}
	if values[0] == nil && values[1] == nil {
		return 0, false, fmt.Errorf("no member '%s' found for key '%s'", member, key)
	}
	if values[0] != nil && values[1] != nil {
		return 0, false, fmt.Errorf("member '%s' of key '%s' seems to be deleted and inserted at the same time", member, key)
	}

	var score float64
	var wasInserted bool
	if values[0] != nil {
		wasInserted = true
		score, err = redis.Float64(values[0], nil)
	} else {
		wasInserted = false
		score, err = redis.Float64(values[1], nil)
	}
	return score, wasInserted, err
}
