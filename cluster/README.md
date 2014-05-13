# cluster

[![GoDoc](https://godoc.org/github.com/soundcloud/roshi/cluster?status.png)](https://godoc.org/github.com/soundcloud/roshi/cluster)

Package cluster provides an Insert/Select/Delete API on top of a single
[Pool object][pool]. Cluster accepts KeyScoreMember tuples from clients,
which map to [Redis sorted set semantics][zset]. Briefly, **key** identifies
the set, **member** the element in the set, and **score** is a sort of version
associated with the element. Write (Insert or Delete) operations are no-ops if
the passed score is less than or equal to any previously written score for
that member.

[pool]: https://godoc.org/github.com/soundcloud/roshi/pool#Pool
[zset]: http://redis.io/commands#sorted-set

All elements of the KeyScoreMember tuple are expected to be provided by the
client. As scores are typically timestamps, an important consideration is that
cluster is totally reliant on an external clock.

## Associative, commutative, idempotent

To ensure a given Member is only represented by the highest Score in the
logical set identified by Key, and to ensure the correct semantics for
Deletes (i.e. that a newer Delete takes precedence over an older Insert)
cluster uses a [Redis script][scripting] to manage membership in two physical
sets: an "Inserts" set `key+`; and a "Deletes" set `key-`. The design is
facially similar to the [2P-set CRDT][2p-set].

[scripting]: http://redis.io/commands#scripting
[2p-set]: https://github.com/aphyr/meangirls#2p-set

In pseudocode, that script is:

```
bool valid(key, score, member):
	if contains(key+, member) and score < score_of(key+, member):
		return false
	if contains(key-, member) and score <= score_of(key-, member):
		return false
	return true

insert(key, score, member):
	if valid(key, score, member):
		add(key+, member, score)
		del(key-, member)

delete(key, score, member):
	if valid(key, score, member):
		add(key-, member, score)
		del(key+, member)
```

Script execution is atomic, and a single logical key is deterministically
stored on a single node. These properties ensure that every possible finite
set of (WriteOp + KeyScoreMember) operations resolves to the same final state,
regardless of the execution order. (I think another way of stating that is
that any valid [linearization][aphyr] of write operations is equal to any of
that linearization's permutations. Feedback on this point would be
appreciated.) Interleaved reads will always return correct data from the
perspective of that specific history.

[aphyr]: http://aphyr.com/posts/309-knossos-redis-and-linearizability

It's also interesting to note that [Select][select] returns a complete
KeyScoreMember tuple. If we have some mechanism of detecting inconsistent data
via reads—for example, by intelligently comparing results from multiple
clusters—we immediately have enough information to issue repairing write
operations against the inconsistent clusters.

[select]: http://godoc.org/github.com/soundcloud/roshi/cluster#Select
