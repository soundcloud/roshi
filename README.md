# roshi

Roshi implements a time-series event storage via a LWW-element-set CRDT with
inline garbage collection. Roshi is a stateless, distributed layer on top of
Redis and is implemented in Go. It is partition tolerant, highly available and
eventually consistent.

At a high level, Roshi maintains sets of values, with each set ordered
according to (external) timestamp, newest-first. Roshi provides the following
API:

* Insert(key, timestamp, value)
* Delete(key, timestamp, value)
* Select(key, offset, limit) []TimestampValue

Roshi stores a sharded copy of your dataset in multiple independent Redis
instances, called a **cluster**. Roshi provides fault tolerance by duplicating
clusters; multiple identical clusters, normally at least 3, form a **farm**.
Roshi leverages CRDT semantics to ensure consistency without explicit
consensus.

# Theory and system properties

Roshi is a distributed system, for two reasons: it's made for datasets that
don't fit on one machine, and it's made to be tolerant against node failure.

Next, we will explain some of the design and properties of the system.

## CRDT

CRDTs (convergent replicated data types) are data types on which the same set
of operations yields the same outcome, regardless of order of execution and
duplication of operations. This allows data convergence without the need for
consenus between replicas. In turn, this allows for easier implementation (no
consensus protocol implementation) as well as lower latency (no wait-time for
consensus).

Operations on CRDTs need to adher [to the following rules][mixu]:

- Associativity (a+(b+c)=(a+b)+c), so that grouping doesn't matter
- Commutativity (a+b=b+a), so that order of application doesn't matter
- Idempotence (a+a=a), so that duplication does not matter

Data types as well as operations have to be specifically crafted to meet these
rules. CRDTs have known implementations for (among others) counters,
registers, sets, and graphs. Roshi implements a set data type, specifically
the Last-Writer-Wins-element-set (LWW-element-set).

This is an intuitive description of the LWW-element-set:

* An element is in the set, if its most-recent operation was an ADD
* An element is not in the set, if its most-recent operation was a REMOVE

A more formal description of a LWW-element-set, as informed by
[Shapiro][shapiro], is as follows: A set S is represented by two internal
sets, the ADD set A and the REMOVE set R. To add an element e to the set S,
add a tuple t with the element and the current timestamp t=(e, now()) to A. To
remove an element from the set S, add a tuple t with the element and the
current timestamp t=(e, now()) to R. To check if an element e is in the set S,
check if it is in the ADD set A and not in the REMOVE set R with a higher
timestamp.

Roshi implements the above definition, but extends it by applying instant
garbage collection. When adding an element e to the set, check if the element
is already in the REMOVE set. If so, check the REMOVE set element timestamp.
If the REMOVE SET element timestamp is lower than the new element timestamp,
delete the element from the REMOVE set and add the new element to the ADD set.
If the REMOVE SET element timestamp is higher than the new element timestamp,
do nothing. The same process is applied with interchanged REMOVE/ADD SETs when
removing an element. In contrast to the formal description above, the Roshi
approach allows for the same element to be added again (with a higher
timestamp) after it's been removed.

Below are all possible combinations of add and remove operations.
A(elements...) is the state of the ADD set. R(elements ...) is the state of
the REMOVE set. An element is a tuple with (value, timestamp). add(element)
and remove(element) are the operations.

    A(a,1)R() + add(a,0)    = A(a,1)R()
    A(a,1)R() + add(a,1)    = A(a,1)R()
    A(a,1)R() + add(a,2)    = A(a,2)R()

    A(a,1)R() + remove(a,0) = A(a,1)R()
    A(a,1)R() + remove(a,1) = A(a,1)R()
    A(a,1)R() + remove(a,2) = A()R(a,2)

    A()R(a,1) + add(a,0)    = A(a,1)R()
    A()R(a,1) + add(a,1)    = A(a,1)R()
    A()R(a,1) + add(a,2)    = A()R(a,2)

    A()R(a,1) + remove(a,0) = A()R(a,1)
    A()R(a,1) + remove(a,1) = A()R(a,1)
    A()R(a,1) + remove(a,2) = A()R(a,2)

An element will always be in either the ADD or the REMOVE set exclusively, but
never in both and never more than once. This results in the set S being the
same as the ADD set A.

Every key in Roshi represents a set. Each set is its own LWW-element-set.

For more information on CRDTs, the following resources might be helpful:

- [The chapter on CRDTs][mixu] in "Distributed Systems for Fun and Profit" by Mixu
- "[A comprehensive study of Convergent and Commutative Replicated Data Types][shapiro]" by Mark Shapiro et al. 2011

[mixu]: http://book.mixu.net/distsys/eventual.html
[shapiro]: http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf

## Replication

Roshi replicates data over several clusters (see the chapter on
[architecture][#architecture]). A typical replication factor is 3. Each Roshi
instance can serve all requests (Insert, Delete, Select) for a client. Roshi
has two methods of replicating data: during write, and during read-repair.

A write (Insert or Delete) is sent to all clusters. It returns success the
moment more than half (N/2 + 1) of the clusters return success. Unsuccessful
clusters might either have been too slow (but still accepted the write) or
failed (due to a network partition or an instance crash). In case of failure,
read-repair might be triggered on a later read.

A read (Select) is dependent on the read strategy employed. If the strategy
queries several clusters, it might be able to spot disagreement in the
returned sets. If so, the unioned set is returned to the client, and in the
background, a read-repair is triggered, which lazily converges the sets across
all replicas.

[Package farm][farm] explains read strategies and read-repair further.

[farm]: http://github.com/soundcloud/roshi/tree/master/farm

## Fault tolerance

Roshi runs as a homogenous distributed system. Each Roshi instance can serve
all requests (Insert, Delete, Select) for a client, and communicates with all
Redis instances.

A Roshi instance is effectively stateless, but holds transient state. If a
Roshi instance crashes, two types of state are lost:

1. Current client connections are lost. Clients can reconnect to another Roshi
   instance and re-execute their operation.
2. Unresolved read-repairs are lost. The read-repair might be triggered again
   during another read.

Since all operations are idempotent, both failure modes do not impede on
convergence of the data.

Persistence is handled by [Redis][redis-persistence]. Data on a
crashed-but-recovered Redis instance might be lost between the time it
commited to disk, and the time it accepts connections again. The lost data gap
might be repaired via read-repair.

[redis-persistence]: http://redis.io/topics/persistence

If a Redis instance is permanently lost and has to be replaced with a fresh
instance, there are two options:

1. Replace it with an empty instance. Keys will be replicated to it via
   read-repair. As more and more keys are replicated, the read-repair load will
   decrease and the instance will work normally. This process might result in
   data loss over the lifetime of a system: if the other replicas are also
   lost, not replicated keys (keys that have not been requested and thus did
   not trigger a read-repair) are lost.
2. Replace it with a cloned replica. There will be a gap between the time of
   the last write respected by the replica and the first write respected by the
   new instance. This gap might be fixed by subsequent read-repairs.

These properties and procedures warrant careful consideration.

## Considerations

### Elasticity

Roshi does not support elasticity. It is not possible to change the sharding
configuration during operations. Roshi has static service discovery,
configured during startup.

### Data structure

Roshi works with LWW-element-sets only. Clients might choose to model other
data types on top of the LWW-element-sets themselves.

### Correct client timestamps

Client timestamps are assumed to correctly represent the physical order of
events coming into the system. Incorrect client timestamps might lead to
values of a client either never appearing or always overriding other values in
a set.

### Data loss

Assuming a replication factor of 3, Roshi makes the following guarantees in
the presence of failures of Redis instances that represent the same data
shard:

Failures | Data loss? | Reads                              | Writes
---------|------------|------------------------------------|----------
1        | No         | Success dependent on read strategy | Succeed
2        | No         | Success dependent on read strategy | Fail
3        | Yes        | Fail                               | Fail

[Package farm][farm] explains read strategies further.

Failures of Redis instances over independent data shards don't affect
instantaneous data durability. However, over time, independent Redis instance
failures can lead to data loss, especially on keys which are not regularly
read-repaired.
In practice, a number of strategies may be used to probabilisticly mitigate
this concern. For example, walking modified keys after known outages, or the
whole keyspace at regular intervals, which will trigger read-repairs for
inconsistent sets.
However, **Roshi fundamentally does not guarantee perfect data durability**.
Therefore, Roshi should not be used as a source of truth, but only as an
intermediate store for performance critical data.

# Architecture

Roshi has a layered architecture, with each layer performing a specific
job with a relatively small surface area. From the bottom up...

## Redis

Roshi is ultimately implemented on top of Redis instance(s), utilizing the
[sorted set][sorted-set] data type. For more details on how the sorted sets
are used, see package cluster.

[sorted-set]: http://redis.io/commands#sorted_set

## shard

[Package shard][shard] performs key-based sharding over one or more Redis
instances. It exposes basically a single method, taking a key and yielding a
connection to the Redis instance that should hold that key. All Redis
interactions go through package shard.

For the sake of implementation simplicity, package shard currently provides no
runtime elasticity, i.e. resharding. To grow or shrink a cluster, clients are
expected to boot up a new cluster of the desired size, and re-materialize
their data from its authoritative source. Clients should build applications on
top of Roshi with this requirement in mind: bulk-loading should be fast and
easy.

[shard]: http://github.com/soundcloud/roshi/tree/master/shard

## cluster

[Package cluster][cluster] implements an Insert/Select/Delete API on top of
package shard. To ensure idempotency and [commutativity][commutativity],
package cluster expects timestamps to arrive as float64s, and refuses writes
with smaller timestamps than what's already been persisted. To ensure
information isn't lost via deletes, package cluster maintains two physical
Redis sorted sets for every logical (user) key, and manages the transition of
key-timestamp-value tuples between those sets.

Package cluster provides and manages the sharding of a single copy of your
dataset.

[cluster]: http://github.com/soundcloud/roshi/tree/master/cluster
[commutativity]: http://en.wikipedia.org/wiki/Commutative_property

## farm

[Package farm][farm] implements a single Insert/Select/Delete API over multiple
underlying clusters. Writes (Inserts and Deletes) are sent to all clusters, and
a quorum is required for success. Reads (Selects) abide one of several read
strategies. Some read strategies allow for the possibility of read-repair.

Package farm provides and manages safety and availiablity requirements of your
complete dataset.

[farm]: http://github.com/soundcloud/roshi/tree/master/farm

## roshi-server

[roshi-server][roshi-server] makes a Roshi farm accessible through a REST-ish
HTTP interface.

* Insert: HTTP POST, body with JSON-encoded key-score-members array
* Select: HTTP GET, body with JSON-encoded keys array
* Delete: HTTP DELETE, body with JSON-encoded key-score-members array

[roshi-server]: http://github.com/soundcloud/roshi/tree/master/roshi-server

## The big picture

```
+-Farm-------------------------------------------------------------------+
| +-Cluster------------+  +-Cluster------------+  +-Cluster------------+ |
| | +-Shards---------+ |  | +-Shards---------+ |  | +-Shards---------+ | |
| | | Redis instance | |  | | Redis instance | |  | | Redis instance | | |
| | | Redis instance | |  | | Redis instance | |  | | Redis instance | | |
| | | Redis instance | |  | | Redis instance | |  | | Redis instance | | |
| | | Redis instance | |  | | Redis instance | |  | | Redis instance | | |
| | +----------------+ |  | +----------------+ |  | +----------------+ | |
| +--------------------+  +--------------------+  +--------------------+ |
+------------------------------------------------------------------------+
```

(Clusters need not have the same number of Redis instances.)

# Development

Roshi is written in [Go](http://golang.org). You'll need a recent version of
Go installed on your computer to build Roshi. If you're on a Mac and use
[homebrew](http://brew.sh), `brew install go` should work fine.

## Build

    go build ./...

## Test

    go test ./...

# I just want to run the HTTP service!

    go get github.com/soundcloud/roshi/roshi-server
    roshi-server -h
