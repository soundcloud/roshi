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

# Theory

Roshi is implemented as a Distributed System. There are two reasons for that:
1) it is made for data sizes that do not fit on one machine and 2) is it made
to be tolerant against node failure.

Next we will explain some of the design decisions.

## CRDT

CRDTs (convergent replicated datatypes) are datatypes, on which the same set
of operations yields the same outcome, regardless of order of execution and
duplication of operations. This allows data convergence without the need for
consenus between replicas. In turn, this allows for easier implementation (no
consensus protocol implementation) as well as lower latency (no wait-time for
consensus).

Operations on CRDTs need to adher to the following rules:

* Associativity (a+(b+c)=(a+b)+c), so that grouping doesn't matter
* Commutativity (a+b=b+a), so that order of application doesn't matter
* Idempotence (a+a=a), so that duplication does not matter

Datatypes as well as operations have to be specifically crafted to meet these
rules. CRDTs have known implementations for, among others, counters,
registers, sets and graphs. Roshi implements a set datatype, specifically the
Last-Writer-Wins-element-set (LWW-element-set).

This is an intuitive description of the LWW-element-set:

* An element is in the set, if its most-recent operation was an ADD
* An element is not in the set, if its most-recent operation was a REMOVE

This is a more formal description of a LWW-element-set as defined by
[shapiro][shapiro]. A set S is represented by two internal sets, the ADD set A
and the REMOVE set R. To add an element e to the set S, add a tupel t with the
element and the current timestamp t=(e, now()) to A. To remove an element from
the set S, add a tupel t with the element and the current timestamp t=(e,
now()) to R. To check if an element e is in the set S, check if it is in the
ADD set A and not in the REMOVE set R with a higher timestamp.

Roshi implements the above mentioned definition, but extends it by applying
instant garbage collection. When adding an element e to the set, check if the
element is already in the REMOVE set. If so, check the REMOVE set element
timestamp. If the REMOVE SET element timestamp is lower than the new element
timestamp, delete the element from the REMOVE set and add the new element to
the ADD set. If the REMOVE SET element timestamp is higher than the new
element timestamp, do nothing. The same process is applied with interchanged
REMOVE/ADD SETs when removing an element.

Below are all possible combinations of add and remove operations.
A(elements...) is the state of the ADD set. R(elements ...) is the state of
the REMOVE set. An element is a tuple with (value, timestamp). add(element)
and remove(element) are the operations.

```
A(a,1)R() + add(a,0) = A(a,1)R()
A(a,1)R() + add(a,1) = A(a,1)R()
A(a,1)R() + add(a,2) = A(a,2)R()

A(a,1)R() + remove(a,0) = A(a,1)R()
A(a,1)R() + remove(a,1) = A(a,1)R()
A(a,1)R() + remove(a,2) = A()R(a,2)

A()R(a,1) + add(a,0) = A(a,1)R()
A()R(a,1) + add(a,1) = A(a,1)R()
A()R(a,1) + add(a,2) = A()R(a,2)

A()R(a,1) + remove(a,0) = A(a,1)R()
A()R(a,1) + remove(a,1) = A(a,1)R()
A()R(a,1) + remove(a,2) = A()R(a,2)
```

An element will always be in either the ADD or the REMOVE set exclusively, but
never in both and never more than once. This results in the set S being the
same as the ADD set A.

Every key in Roshi holds a set. Each of these sets is an own LWW-element-set.

For more information on CRDTs, the following resources might be helpful:

* The chapter on CRDTs in "Distributed Systems for Fun and Profit" by Mixu
http://book.mixu.net/distsys/eventual.html
* "A comprehensive study of Convergent and Commutative Replicated Data Types"
by Mark Shapiro et al. 2011
http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf

[shapiro]: http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf

## Replication

Roshi replicates data over several clusters (see chapter on
[architecture][#architecture]). A typical replication factor is 3. Each Roshi
instances can server all request (Insert, Delete, Select) for a client. Roshi
has two paths of how data is replicated, during write and during read-repair.

A write (Insert or Delete) is sent to all clusters. It immediately returns
successfull, when more than half of replicas returned successfull.
Unsuccessful clusters might either have been too slow (but still accepted the
write) or failed (due to a network partition or an instance crash). In case of
failure, read-repair might be triggered on a later read.

A read (Select) is dependent on the read strategy employed. If it requests
several clusters, it might be able to spot disagreement in the returned set.
If so, the unioned set is returned to the client. In the background, a
read-repair will be triggered. It will query all clusters' ADD and REMOVE sets
for that key, union them and distribute them back to all clusters. Due to the
CRDT semantics of the LWW-element-set, this makes all clusters converge to the
same set state.

The [farm][farm] package explains read strategies and read-repair further.

[farm]: http://github.com/soundcloud/roshi/tree/master/farm

## Fault tolerance

Roshi runs as a homogenous distributed system. Each Roshi instance can accept
all request (Insert, Delete, Select) and communicates with all Redis
instances.

A Roshi instance is practically stateless, but holds transient state. If a
Roshi instance crashes:

* current client connections are lost. Clients can reconnect to another Roshi
  instance and re-execute their operation.
* read-repairs processes are lost. The read-repair might be triggered again
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

* replace it with an empty instance. Keys will be replicated to it via
  read-repair. As more and more keys are replicated, the read-repair load will
  decrease and the instance will work normally. This process might result in
  data loss over the lifetime of a system: if the other replicas are also
  lost, not replicated keys (keys that have not been requested and thus did
  not trigger a read-repair) are lost.
* replace it with an cloned replica. There will be a gap between the time of
  the last write respected by the replica and the first write respected by the
  new instance. This gap might be fixed by subsequent read-repairs.

## Considerations

*Elasticity* Roshi does not support elasticity. It is not possible to change
the sharding during operations. Roshi has static service discovery,
configurated during startup.

*Data structure* Roshi works with LWW-element-sets only. Clients might choose
to model other data types on top of the LWW-element-sets themselves.

*Correct client timestamps* Client timestamps are assumed to correctly
represent the physical order of events coming into the system. Incorrect
client timestamps might lead to values of a client either never appearing or
always overriding other values in a set.

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
| | +----------------+ |  | | Redis instance | |  | | Redis instance | | |
| +--------------------+  | | Redis instance | |  | | Redis instance | | |
|                         | | Redis instance | |  | +----------------+ | |
|                         | | Redis instance | |  +--------------------+ |
|                         | +----------------+ |                         |
|                         +--------------------+                         |
+------------------------------------------------------------------------+
```

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

