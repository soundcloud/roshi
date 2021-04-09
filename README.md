# roshi [![Build Status](https://travis-ci.org/soundcloud/roshi.png)](https://travis-ci.org/soundcloud/roshi) [![GoDoc](https://godoc.org/github.com/soundcloud/roshi?status.svg)](http://godoc.org/github.com/soundcloud/roshi)

Roshi implements a time-series event storage via a LWW-element-set CRDT with
limited inline garbage collection. Roshi is a stateless, distributed layer on
top of Redis and is implemented in Go. It is partition tolerant, highly
available and eventually consistent.

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

# Use cases

Roshi is basically a high-performance **index** for timestamped data. It's
designed to sit in the critical (request) path of your application or service.
The originating use case is the SoundCloud stream; see [this blog post][blog]
for details.

[blog]: http://developers.soundcloud.com/blog/roshi-a-crdt-system-for-timestamped-events

# Theory and system properties

Roshi is a distributed system, for two reasons: it's made for datasets that
don't fit on one machine, and it's made to be tolerant against node failure.

Next, we will explain the system design.

## CRDT

CRDTs (conflict-free replicated data types) are data types on which the same 
set of operations yields the same outcome, regardless of order of execution 
and duplication of operations. This allows data convergence without the need 
for consensus between replicas. In turn, this allows for easier implementation 
(no consensus protocol implementation) as well as lower latency (no wait-time 
for consensus).

Operations on CRDTs need to adhere [to the following rules][mixu]:

- Associativity (a+(b+c)=(a+b)+c), so that grouping doesn't matter.
- Commutativity (a+b=b+a), so that order of application doesn't matter.
- Idempotence (a+a=a), so that duplication doesn't matter.

Data types as well as operations have to be specifically crafted to meet these
rules. CRDTs have known implementations for counters, registers, sets, graphs,
and others. Roshi implements a set data type, specifically the Last Writer
Wins element set (LWW-element-set).

This is an intuitive description of the LWW-element-set:

- An element is in the set, if its most-recent operation was an add.
- An element is not in the set, if its most-recent operation was a remove.

A more formal description of a LWW-element-set, as informed by
[Shapiro][shapiro], is as follows: a set S is represented by two internal
sets, the add set A and the remove set R. To add an element e to the set S,
add a tuple t with the element and the current timestamp t=(e, now()) to A. To
remove an element from the set S, add a tuple t with the element and the
current timestamp t=(e, now()) to R. To check if an element e is in the set S,
check if it is in the add set A and not in the remove set R with a higher
timestamp.

Roshi implements the above definition, but extends it by applying a sort of
instant garbage collection.  When inserting an element E to the logical set S,
check if E is already in the add set A or the remove set R. If so, check the
existing timestamp. If the existing timestamp is **lower** than the incoming
timestamp, the write succeeds: remove the existing (element, timestamp) tuple
from whichever set it was found in, and add the incoming (element, timestamp)
tuple to the add set A. If the existing timestamp is higher than the incoming
timestamp, the write is a no-op.

Below are all possible combinations of add and remove operations.
A(elements...) is the state of the add set. R(elements...) is the state of
the remove set. An element is a tuple with (value, timestamp). add(element)
and remove(element) are the operations.

Original state | Operation   | Resulting state
---------------|-------------|-----------------
A(a,1) R()     | add(a,0)    | A(a,1) R()
A(a,1) R()     | add(a,1)    | A(a,1) R()
A(a,1) R()     | add(a,2)    | A(a,2) R()
A(a,1) R()     | remove(a,0) | A(a,1) R()
A(a,1) R()     | remove(a,1) | A(a,1) R()
A(a,1) R()     | remove(a,2) | A() R(a,2)
A() R(a,1)     | add(a,0)    | A() R(a,1)
A() R(a,1)     | add(a,1)    | A() R(a,1)
A() R(a,1)     | add(a,2)    | A(a,2) R()
A() R(a,1)     | remove(a,0) | A() R(a,1)
A() R(a,1)     | remove(a,1) | A() R(a,1)
A() R(a,1)     | remove(a,2) | A() R(a,2)

For a Roshi LWW-element-set, an element will always be in either the add or
the remove set exclusively, but never in both and never more than once. This
means that the logical set S is the same as the add set A.

Every key in Roshi represents a set. Each set is its own LWW-element-set.

For more information on CRDTs, the following resources might be helpful:

- [The chapter on CRDTs][mixu] in "Distributed Systems for Fun and Profit" by Mixu
- "[A comprehensive study of Convergent and Commutative Replicated Data Types][shapiro]" by Mark Shapiro et al. 2011

[mixu]: http://book.mixu.net/distsys/eventual.html
[shapiro]: http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf

## Replication

Roshi replicates data over several non-communicating clusters. A typical
replication factor is 3. Roshi has two methods of replicating data: during
write, and during read-repair.

A write (Insert or Delete) is sent to all clusters. The overall operation
returns success the moment a user-defined number of clusters return success.
Unsuccessful clusters might either have been too slow (but still accepted the
write) or failed (due to a network partition or an instance crash). In case of
failure, read-repair might be triggered on a later read.

A read (Select) is dependent on the read strategy employed. If the strategy
queries several clusters, it might be able to spot disagreement in the
returned sets. If so, the unioned set is returned to the client, and in the
background, a read-repair is triggered, which lazily converges the sets across
all replicas.

[Package farm][farm] explains replication, read strategies, and read-repair
further.

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

Persistence is delegated to [Redis][redis-persistence]. Data on a
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
   lost, non-replicated keys (keys that have not been requested and thus did
   not trigger a read-repair) are lost.
2. Replace it with a cloned replica. There will be a gap between the time of
   the last write respected by the replica and the first write respected by the
   new instance. This gap might be fixed by subsequent read-repairs.

Both processes can be expedited via a [keyspace walker process][roshi-walker].
Nevertheless, these properties and procedures warrant careful consideration.

## Responses to write operations

Write operations (insert or delete) return boolean to indicate whether the
operation was successfully applied to the data layer, respecting the
configured write quorum. Clients should interpret a write response of false to
mean they should re-submit their operation. A write response of true does
**not** imply the operation mutated the state in a way that will be visible to
readers, merely that it was accepted and processed according to CRDT
semantics.

As an example, all of these write operations would return true.

Write operation         | Final state           | Operation description
------------------------|-----------------------|---------------
Insert("foo", 3, "bar") | foo+ bar/3<br/>foo- — | Initial write
Insert("foo", 3, "bar") | foo+ bar/3<br/>foo- — | No-op: incoming score doesn't beat existing score
Delete("foo", 2, "bar") | foo+ bar/3<br/>foo- — | No-op: incoming score doesn't beat existing score
Delete("foo", 4, "bar") | foo+ —<br/>foo- bar/4 | "bar" moves from add set to remove set
Delete("foo", 5, "bar") | foo+ —<br/>foo- bar/5 | score of "bar" in remove set is incremented

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

Assuming a replication factor of 3, and a write quorum of 2 nodes, Roshi makes
the following guarantees in the presence of failures of Redis instances that
represent the same data shard:

Failures | Data loss? | Reads                              | Writes
---------|------------|------------------------------------|----------
0        | No         | Succeed                            | Succeed
1        | No         | Success dependent on read strategy | Succeed
2        | No         | Success dependent on read strategy | Fail
3        | Yes        | Fail                               | Fail

[Package farm][farm] explains read strategies further.

Failures of Redis instances over independent data shards don't affect
instantaneous data durability. However, over time, independent Redis instance
failures can lead to data loss, especially on keys which are not regularly
read-repaired.
In practice, a number of strategies may be used to probabilistically mitigate
this concern. For example, walking modified keys after known outages, or the
whole keyspace at regular intervals, which will trigger read-repairs for
inconsistent sets.
However, **Roshi fundamentally does not guarantee perfect data durability**.
Therefore, Roshi should not be used as a source of truth, but only as an
intermediate store for performance critical data.

### Authentication, authorization, validation

In case it's not obvious, Roshi performs no authentication, authorization, or
any validation of input data. Clients must implement those things themselves.

# Architecture

Roshi has a layered architecture, with each layer performing a specific
job with a relatively small surface area. From the bottom up...

- **Redis**: Roshi is ultimately implemented on top of Redis instance(s),
  utilizing the [sorted set][sorted-set] data type. For more details on how
  the sorted sets are used, see package cluster, below.

- **[Package pool][pool]** performs key-based sharding over one or more Redis
  instances. It exposes basically a single method, taking a key and yielding a
  connection to the Redis instance that should hold that key. All Redis
  interactions go through package pool.

- **[Package cluster][cluster]** implements an Insert/Select/Delete API on top
  of package pool. To ensure idempotency and [commutativity][commutativity],
  package cluster expects timestamps to arrive as float64s, and refuses writes
  with smaller timestamps than what's already been persisted. To ensure
  information isn't lost via deletes, package cluster maintains two physical
  Redis sorted sets for every logical (user) key, and manages the transition of
  key-timestamp-value tuples between those sets.

- **[Package farm][farm]** implements a single Insert/Select/Delete API over
  multiple underlying clusters. Writes (Inserts and Deletes) are sent to all
  clusters, and a quorum is required for success. Reads (Selects) abide one of
  several read strategies. Some read strategies allow for the possibility of
  read-repair.

- **[roshi-server][roshi-server]** makes a Roshi farm accessible through a
  REST-ish HTTP interface. It's effectively stateless, and [12-factor][twelve]
  compliant.

- **[roshi-walker][roshi-walker]** walks the keyspace in semirandom order at a
  defined rate, making Select requests for each key in order to trigger read
  repairs.

[sorted-set]: http://redis.io/commands#sorted_set
[pool]: http://github.com/soundcloud/roshi/tree/master/pool
[cluster]: http://github.com/soundcloud/roshi/tree/master/cluster
[commutativity]: http://en.wikipedia.org/wiki/Commutative_property
[farm]: http://github.com/soundcloud/roshi/tree/master/farm
[roshi-server]: http://github.com/soundcloud/roshi/tree/master/roshi-server
[twelve]: http://12factor.net
[roshi-walker]: http://github.com/soundcloud/roshi/tree/master/roshi-walker

## The big picture

![Overview](http://i.imgur.com/SEeKquW.png)

(Clusters need not have the same number of Redis instances.)

# Development

Roshi is written in [Go](http://golang.org). You'll need a recent version of
Go installed on your computer to build Roshi. If you're on a Mac and use
[homebrew](http://brew.sh), `brew install go` should work fine.

## Build

    go build ./...

## Test

    go test ./...

# Running

See [roshi-server][roshi-server] and [roshi-walker][roshi-walker] for
information about owning and operating your own Roshi.
