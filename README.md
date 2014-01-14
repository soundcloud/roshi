# roshi

Roshi implements a LWW-element-set CRDT with inline garbage collection for
time-series events.

At a high level, Roshi maintains a bunch of sets of values, with each set
ordered according to (external) timestamp, newest-first. Roshi provides the
following API:

* Insert(key, timestamp, value)
* Delete(key, timestamp, value)
* Select(key, offset, limit) []TimestampValue

(Very short description of Roshi's consistency guarantees/data model here.)

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
a single shard. To ensure idempotency, package cluster expects timestamps to
arrive as float64s, which it interprets as versions, and refuses writes with
smaller versions than what's already been persisted. To ensure information
isn't lost via deletes, package cluster maintains two physical Redis sorted
sets for every logical (user) key, and manages the transition of key-version-
value tuples between those sets.

Package cluster provides and manages the sharding and distribution of your
data set.

[cluster]: http://github.com/soundcloud/roshi/tree/master/cluster

## farm

[Package farm][farm] implements a single Insert/Select/Delete API over multiple
underlying clusters. Writes (Inserts and Deletes) are sent to all clusters, and
a quorum is required for success. Reads (Selects) abide one of several read
strategies. Some read strategies allow for the possibility of read-repair.

Package farm provides and manages safety and availiablity requirements of your
data set.

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

# Theory

(Big section here.)

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

