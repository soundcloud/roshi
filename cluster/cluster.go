// Package cluster provides a sorted-set API (via Redis ZSETs) on top of a
// group of Redis instances.
package cluster

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/soundcloud/roshi/common"
	"github.com/soundcloud/roshi/instrumentation"
	"github.com/soundcloud/roshi/pool"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

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

// Selecter defines the methods to retrieve elements from a sorted set.
type Selecter interface {
	SelectOffset(keys []string, offset, limit int) <-chan Element
	SelectRange(keys []string, start, stop common.Cursor, limit int) <-chan Element
}

// Deleter defines the method to delete elements from a sorted set. A key-
// member's score must be larger than the currently stored score for the delete
// to be accepted. A non-nil error indicates only physical problems, not
// logical.
type Deleter interface {
	Delete(tuples []common.KeyScoreMember) error
}

// Scorer defines the method to retrieve the presence information of a set of
// key-members.
type Scorer interface {
	Score([]common.KeyMember) (map[common.KeyMember]Presence, error)
}

// Scanner emits all keys in the keyspace over a returned
// channel. When the keys are exhaused, the channel is closed. The
// order in which keys are emitted is unpredictable. Scanning is
// performed one Redis instance at a time in random order of the
// instances. If an instance is down at the time it is tried to be
// scanned, it is skipped (no retries). See also implications of the
// Redis SCAN command. Note that keys for which only deletes have
// happened (and no inserts) will not be emitted.
type Scanner interface {
	Keys(batchSize int) <-chan []string
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
		elseif deleteTs and tonumber(ARGV[1]) <= tonumber(deleteTs) then
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
	pool            *pool.Pool
	maxSize         int
	selectGap       time.Duration
	instrumentation instrumentation.Instrumentation
}

// New creates and returns a new Cluster backed by a concrete Redis cluster.
// maxSize for each key will be enforced at write time. selectGap specifies a
// wait period between pipeline calls to individual connections within a pool
// when performing a Select with multiple keys. Instrumentation may be nil.
func New(pool *pool.Pool, maxSize int, selectGap time.Duration, instr instrumentation.Instrumentation) Cluster {
	if instr == nil {
		instr = instrumentation.NopInstrumentation{}
	}
	return &cluster{
		pool:            pool,
		maxSize:         maxSize,
		selectGap:       selectGap,
		instrumentation: instr,
	}
}

// Insert efficiently performs ZADDs for each of the passed tuples.
func (c *cluster) Insert(keyScoreMembers []common.KeyScoreMember) error {
	// Bucketize
	m := map[int][]common.KeyScoreMember{}
	for _, tuple := range keyScoreMembers {
		index := c.pool.Index(tuple.Key)
		m[index] = append(m[index], tuple)
	}

	// Scatter
	errChan := make(chan error, len(m))
	for index, keyScoreMembers := range m {
		go func(index int, keyScoreMembers []common.KeyScoreMember) {

			errChan <- c.pool.WithIndex(index, func(conn redis.Conn) error {
				return pipelineInsert(conn, keyScoreMembers, c.maxSize)
			})

		}(index, keyScoreMembers)
	}

	// Gather
	for _ = range m {
		if err := <-errChan; err != nil {
			return err
		}
	}
	return nil
}

// SelectOffset efficiently performs ZREVRANGEs for each of the passed keys
// using the offset and limit for each. It pushes results to the returned chan
// as they become available.
func (c *cluster) SelectOffset(keys []string, offset, limit int) <-chan Element {
	return c.selectCommon(keys, func(conn redis.Conn, myKeys []string) (map[string][]common.KeyScoreMember, error) {
		return pipelineRange(conn, myKeys, offset, limit)
	})
}

// SelectRange uses ZREVRANGEBYSCORE to do a cursor-based select, similar to
// SelectOffset.
func (c *cluster) SelectRange(keys []string, start, stop common.Cursor, limit int) <-chan Element {
	return c.selectCommon(keys, func(conn redis.Conn, myKeys []string) (map[string][]common.KeyScoreMember, error) {
		return pipelineRangeByScore(conn, myKeys, start, stop, limit)
	})
}

func (c *cluster) selectCommon(
	keys []string,
	fn func(redis.Conn, []string) (map[string][]common.KeyScoreMember, error),
) <-chan Element {
	out := make(chan Element)
	go func() {
		// Bucketize
		m := map[int][]string{}
		for _, key := range keys {
			index := c.pool.Index(key)
			m[index] = append(m[index], key)
		}

		// Scatter. We need to return an element for all of the input keys, but it
		// can be an error element. Client does the gathering.
		wg := sync.WaitGroup{}
		wg.Add(len(m))
		delay := time.Duration(0)
		for index, keys := range m {
			go func(index int, keys []string, delay time.Duration) {
				defer wg.Done()
				time.Sleep(delay)

				// Make channel sends outside of this function, to
				// minimize our time with the redis.Conn.
				var elements []Element
				var result map[string][]common.KeyScoreMember
				if err := c.pool.WithIndex(index, func(conn redis.Conn) (err error) {
					result, err = fn(conn, keys)
					return
				}); err != nil {
					elements = errorElements(keys, err)
				} else {
					elements = successElements(result)
				}

				for _, element := range elements {
					out <- element
				}
			}(index, keys, delay)
			delay += c.selectGap
		}
		wg.Wait()

		// Signal that we're done to the client.
		close(out)
	}()
	return out
}

// Delete efficiently performs ZREMs for each of the passed tuples.
func (c *cluster) Delete(keyScoreMembers []common.KeyScoreMember) error {
	// Bucketize
	m := map[int][]common.KeyScoreMember{}
	for _, keyScoreMember := range keyScoreMembers {
		index := c.pool.Index(keyScoreMember.Key)
		m[index] = append(m[index], keyScoreMember)
	}

	// Scatter
	errChan := make(chan error, len(m))
	for index, keyScoreMembers := range m {
		go func(index int, keyScoreMembers []common.KeyScoreMember) {
			errChan <- c.pool.WithIndex(index, func(conn redis.Conn) error {
				return pipelineDelete(conn, keyScoreMembers, c.maxSize)
			})

		}(index, keyScoreMembers)
	}

	// Gather
	for _ = range m {
		if err := <-errChan; err != nil {
			return err
		}
	}
	return nil
}

// Score returns the presence statistics of each passed key-member.
// That is, whether the key-member exists in this cluster, if it's in
// an insert set, and its score.
func (c *cluster) Score(keyMembers []common.KeyMember) (map[common.KeyMember]Presence, error) {
	// Bucketize
	m := map[int][]common.KeyMember{}
	for _, keyMember := range keyMembers {
		index := c.pool.Index(keyMember.Key)
		m[index] = append(m[index], keyMember)
	}

	// Scatter
	type response struct {
		presenceMap map[common.KeyMember]Presence
		err         error
	}
	responseChan := make(chan response, len(m))
	for index, keyMembers := range m {
		go func(index int, keyMembers []common.KeyMember) {
			var presenceMap map[common.KeyMember]Presence
			err := c.pool.WithIndex(index, func(conn redis.Conn) (err error) {
				presenceMap, err = pipelineScore(conn, keyMembers)
				return
			})
			if err != nil {
				log.Printf("cluster: Score: %q: %s", c.pool.ID(index), err)
			}
			responseChan <- response{presenceMap, err}
		}(index, keyMembers)
	}

	// Gather
	presenceMap := map[common.KeyMember]Presence{}
	for i := 0; i < cap(responseChan); i++ {
		response := <-responseChan
		if response.err != nil {
			continue
		}
		for keyMember, presence := range response.presenceMap {
			presenceMap[keyMember] = presence
		}
	}
	return presenceMap, nil
}

// Presence represents the state of a given key-member in a cluster.
type Presence struct {
	Present  bool
	Inserted bool // false = deleted
	Score    float64
}

// Keys implements the Scanner interface.
func (c *cluster) Keys(batchSize int) <-chan []string {
	ch := make(chan []string)
	go func() {
		defer close(ch)

		var sent uint64
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		go func() {
			for _ = range t.C {
				log.Printf("cluster: Keys: sent %d key(s) from all instances", atomic.LoadUint64(&sent))
			}
		}()

		for _, index := range rand.Perm(c.pool.Size()) {
			log.Printf("cluster: scanning keyspace of %q (batch size %d)", c.pool.ID(index), batchSize)
			cursor := 0
			batch := make([]string, 0, batchSize)
			for {
				if err := c.pool.WithIndex(index, func(conn redis.Conn) error {
					values, err := redis.Values(conn.Do("SCAN", cursor, "COUNT", fmt.Sprint(batchSize)))
					if err != nil {
						return err
					}

					if n := len(values); n != 2 {
						return fmt.Errorf("received %d values from Redis, expected exactly 2", n)
					}

					newCursor, err := redis.Int(values[0], nil)
					if err != nil {
						return err
					}

					keys, err := redis.Strings(values[1], nil)
					if err != nil {
						return err
					}

					for _, key := range keys {
						// Only emit keys with insertSuffix - but strip the suffix.
						l := len(key) - len(insertSuffix)
						if key[l:] == insertSuffix {
							batch = append(batch, key[:l])
							if len(batch) >= batchSize {
								atomic.AddUint64(&sent, uint64(len(batch)))
								ch <- batch
								batch = make([]string, 0, batchSize)
							}
						}
					}
					cursor = newCursor
					return nil
				}); err == nil && cursor == 0 {
					log.Printf("cluster: Keys on %q is complete", c.pool.ID(index))
					break // No error, and cursor back at 0: this instance is done.
				} else if err != nil {
					log.Printf("cluster: during Keys on %q: %s", c.pool.ID(index), err)
					time.Sleep(1 * time.Second) // and retry
				}
			}
			if len(batch) > 0 {
				ch <- batch
			}
		}
	}()
	return ch
}

func pipelineInsert(conn redis.Conn, keyScoreMembers []common.KeyScoreMember, maxSize int) error {
	for _, tuple := range keyScoreMembers {
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

	for _ = range keyScoreMembers {
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

func pipelineRange(conn redis.Conn, keys []string, offset, limit int) (map[string][]common.KeyScoreMember, error) {
	if limit < 0 {
		return map[string][]common.KeyScoreMember{}, fmt.Errorf("negative limit is invalid for offset-based select")
	}
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

		var (
			ksm             = common.KeyScoreMember{Key: key}
			keyScoreMembers = make([]common.KeyScoreMember, 0, len(values))
		)

		for len(values) > 0 {
			if values, err = redis.Scan(values, &ksm.Member, &ksm.Score); err != nil {
				return map[string][]common.KeyScoreMember{}, err
			}

			keyScoreMembers = append(keyScoreMembers, ksm)
		}

		m[key] = keyScoreMembers
	}

	return m, nil
}

func pipelineRangeByScore(conn redis.Conn, keys []string, start, stop common.Cursor, limit int) (map[string][]common.KeyScoreMember, error) {
	if limit < 0 {
		// TODO maybe change that
		return map[string][]common.KeyScoreMember{}, fmt.Errorf("negative limit is invalid for cursor-based select")
	}

	// pastStart returns true when the score+member are "past" the cursor
	// (smaller score, larger lexicographically) and can therefore be included
	// in the resultset.
	pastStart := func(score float64, member string) bool {
		if score < start.Score {
			return true
		}
		if score == start.Score && member < start.Member {
			return true
		}
		return false
	}

	// beforeStop returns true as long as the score+member are "before" the
	// stop (larger score, smaller lexicographically) and can therefore
	// be included in the resultset.
	beforeStop := func(score float64, member string) bool {
		if score > stop.Score {
			return true
		}
		if score == stop.Score && member > stop.Member {
			return true
		}
		return false
	}

	// An unlimited number of members may exist at cursor.Score. Luckily,
	// they're in lexicographically stable order. Walk the elements we get
	// back. For as long as element.Score == cursor.Score, and a
	// lexicographical comparison of (element.Score, cursor.Score) < 0,
	// discard the element. As soon as that condition fails, break the loop,
	// and collect elements. If we run out of elements before collecting the
	// user-requested limit, double the limit and try again, up to N times.

	var (
		startScoreStr = fmt.Sprint(start.Score)
		keysToSelect  = keys  // start with all
		selectLimit   = limit // double every time
		maxAttempts   = 4     // up to this many times (TODO could be paramaterized)
		results       = make(map[string][]common.KeyScoreMember, len(keys))
	)

	for attempt := 0; len(keysToSelect) > 0 && attempt < maxAttempts; attempt++ {
		for _, key := range keysToSelect {
			if err := conn.Send(
				"ZREVRANGEBYSCORE",
				key+insertSuffix,
				startScoreStr, // max
				"-inf",        // min
				"WITHSCORES",
				"LIMIT",
				0,
				selectLimit,
			); err != nil {
				return map[string][]common.KeyScoreMember{}, err
			}
		}

		if err := conn.Flush(); err != nil {
			return map[string][]common.KeyScoreMember{}, err
		}

		m := make(map[string][]common.KeyScoreMember, len(keys))
		for _, key := range keysToSelect {
			values, err := redis.Values(conn.Receive())
			if err != nil {
				return map[string][]common.KeyScoreMember{}, err
			}

			var (
				collected = 0
				validated = make([]common.KeyScoreMember, 0, len(values))
				hitStop   = false
				ksm       = common.KeyScoreMember{Key: key}
			)

			for len(values) > 0 && !hitStop {
				if values, err = redis.Scan(values, &ksm.Member, &ksm.Score); err != nil {
					return map[string][]common.KeyScoreMember{}, err
				}

				collected++

				if !pastStart(ksm.Score, ksm.Member) {
					continue // this element is behind or at our start point
				}
				if !beforeStop(ksm.Score, ksm.Member) {
					hitStop = true
					continue // this element is at or beyond our stop point
				}

				validated = append(validated, ksm)
			}

			// At this point, we know if we can use these elements, or need to
			// go back for more.
			var (
				haveEnoughElements = len(validated) >= limit
				exhaustedElements  = collected < selectLimit
			)
			if haveEnoughElements || exhaustedElements || hitStop {
				if len(validated) > limit {
					validated = validated[:limit]
				}
				m[key] = validated
			}
		}

		retryKeys := make([]string, 0, len(keysToSelect))

		for _, key := range keysToSelect {
			if a, ok := m[key]; ok {
				results[key] = a // use it
			} else {
				retryKeys = append(retryKeys, key) // try again
			}
		}

		keysToSelect = retryKeys

		if selectLimit < 10 {
			selectLimit = 25
		} else if selectLimit < 100 {
			selectLimit *= 2
		} else {
			selectLimit += 50
		}
	}

	if n := len(keysToSelect); n > 0 {
		return map[string][]common.KeyScoreMember{}, fmt.Errorf("%d key(s) failed to yield enough elements (original limit %d)", n, limit)
	}

	return results, nil
}

func pipelineDelete(conn redis.Conn, keyScoreMembers []common.KeyScoreMember, maxSize int) error {
	for _, keyScoreMember := range keyScoreMembers {
		if err := deleteScript.Send(
			conn,
			keyScoreMember.Key,
			keyScoreMember.Score,
			keyScoreMember.Member,
			maxSize,
		); err != nil {
			return err
		}
	}

	if err := conn.Flush(); err != nil {
		return err
	}

	for _ = range keyScoreMembers {
		// TODO actually count deletes
		if _, err := conn.Receive(); err != nil {
			return err
		}
	}

	return nil
}

func pipelineScore(conn redis.Conn, keyMembers []common.KeyMember) (map[common.KeyMember]Presence, error) {
	for _, keyMember := range keyMembers {
		if err := conn.Send("ZSCORE", keyMember.Key+insertSuffix, keyMember.Member); err != nil {
			return map[common.KeyMember]Presence{}, err
		}
		if err := conn.Send("ZSCORE", keyMember.Key+deleteSuffix, keyMember.Member); err != nil {
			return map[common.KeyMember]Presence{}, err
		}
	}
	if err := conn.Flush(); err != nil {
		return map[common.KeyMember]Presence{}, err
	}

	m := map[common.KeyMember]Presence{}
	for i := 0; i < len(keyMembers); i++ {
		insertReply, insertErr := conn.Receive()
		insertValue, insertErr := redis.Float64(insertReply, insertErr)
		deleteReply, deleteErr := conn.Receive()
		deleteValue, deleteErr := redis.Float64(deleteReply, deleteErr)
		switch {
		case insertErr == nil && deleteErr == redis.ErrNil:
			m[keyMembers[i]] = Presence{
				Present:  true,
				Inserted: true,
				Score:    insertValue,
			}
		case insertErr == redis.ErrNil && deleteErr == nil:
			m[keyMembers[i]] = Presence{
				Present:  true,
				Inserted: false,
				Score:    deleteValue,
			}
		case insertErr == redis.ErrNil && deleteErr == redis.ErrNil:
			m[keyMembers[i]] = Presence{
				Present: false,
			}
		default:
			return map[common.KeyMember]Presence{}, fmt.Errorf(
				"pipelineScore bad state for %v (%v/%v)",
				keyMembers[i],
				insertErr,
				deleteErr,
			)
		}
	}
	return m, nil
}
