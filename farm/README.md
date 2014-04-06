# farm

[![GoDoc](https://godoc.org/github.com/soundcloud/roshi/farm?status.png)](https://godoc.org/github.com/soundcloud/roshi/farm)

Package farm provides a single logical Insert/Select/Delete API on top of
multiple independent [clusters][cluster]. Package farm ensures that writes
(Insert and Delete) are made to as many clusters as possible, and uses one of
several [read strategies](#read_strategies) to perform reads (Select).

[cluster]: https://github.com/soundcloud/roshi/blob/master/cluster

## Writing

Every write is broadcast to each cluster. As soon as the farm has received a
user-specified number of succesful responses, the overall write is considered
successful, and that success is signaled to the client.

For every single logical key, Roshi maintains two physical keys, representing
add and remove sets. Each write of a key-score-member tuple results in the
scored member existing in exactly one of the physical sets. For more details,
see [package cluster][cluster].

## Reading and read repair

Read requests are processed according to the chosen read strategy. Read
strategies that broadcast the request to more than one cluster have the
opportunity to compare the response set from each cluster, identify
discrepancies, and issue read-repairs.

In the normal case, reading a key queries only the physical add set for that
key. This was a design decision taken for performance reasons: by avoiding the
remove set in most reads, we effectively halve the cost of read queries.

If the read strategy sends the read to multiple clusters, it can perform a
[set union][set-union] ∪ over the returned sets, preferring higher scores when
the same member exists with multiple scores. That union transparently corrects
for clusters that have missed writes. Similarly, the read strategy can perform
a [symmetric difference][symmetric-difference] ∆, which will yield all the
keys with any discrepancies. (Scores and members are irrelevant.) It may then
issue read-repairs.

[set-union]: http://en.wikipedia.org/wiki/Union_(set_theory)
[symmetric-difference]: http://en.wikipedia.org/wiki/Symmetric_difference

Astute readers will note that this behavior biases the farm toward adds: any
incompletely applied delete will have the chance to be present in reads. In
fact, due to the set-union behavior, read strategies that broadcast to more
than one cluster may have a _higher_ chance to incorrectly return deleted
elements than those that read from only one cluster. Again, this was a design
decision taken for performance reasons, and on the assumption that deletes are
significantly less frequent than adds. To solve this problem, read-repairs
always read from both the add and remove sets, to get each cluster's
comprehensive view of the discrepant logical key, and re-issues the correct
write commands to the discrepant clusters.

As an illustration, consider these responses for a read on set S:

```
C1: (A/10 B/20 C/30)
C2: (A/11      C/30)
C3: (A/10      C/30)
```

The set union ∪ is computed as (A/11 B/20 C/30) and returned to the client.
The symmetric difference ∆ is computed as (A B) and sent for comprehensive
read repair. Read repair might detect that member A is present in the add sets
for S on each cluster, but has the highest score (11) only on C2, and so would
reissue an Insert(S, 11, A) to clusters C1 and C3. Similarly, it might detect
that member B is in the delete set for S on C2 and C3 with score 22, but only
in the add set on C1 with score 20, and so would reissue a Delete(S, 22, B) to
cluster C1.

In this way, Roshi becomes eventually consistent.

### Read strategies

#### SendOneReadOne

SendOneReadOne is the simplest (or most naïve) read strategy, and has the
least impact on the network and underlying clusters. It forwards a single read
request to a single randomly-chosen cluster, and waits for the complete
response. It has no way to compute union- or difference-sets, and therefore
performs no read-repair. A complete cluster failure is returned to the client
as an error; otherwise, partial results are returned.

SendOneReadOne is useful for benchmarking and performance testing, but
probably shouldn't be used in production.

#### SendAllReadAll

SendAllReadAll is the safest read strategy. It forwards the read request to
all underlying clusters, waits for all responses, computes union- and
difference-sets for read repair, and finally returns the union-set.

SendAllReadAll is the best read strategy if you can afford to use it, i.e. if
your read volume isn't so high that you overload your infrastructure.

#### SendAllReadFirstLinger

SendAllReadFirstLinger broadcasts the select request to all clusters, waits
for the first non-error response, and returns it directly to the client.
Before returning, SendAllReadFirstLinger spawns a goroutine to linger and
collect responses from all the clusters. When all responses have been
collected, SendAllReadFirstLinger will determine which keys should be sent
for read repair.

SendAllReadFirstLinger is a good read strategy if SendAllReadAll makes your
clients wait too long, and you can tolerate some perceived inconsistency.

#### SendVarReadFirstLinger

SendVarReadFirstLinger is a refined version of SendAllReadFirstLinger. It
works in the same way but reduces the requests to all clusters under certain
circumstances. Specifically, this read strategy caps the number of broadcasts
to all clusters at a configurable amount per second. The surplus requests are
sent to a single cluster, and become "promoted" to all-cluster broadcasts if
they return an error, or fail to yield a response within a configurable
timeout.

SendVarReadFirstLinger is a relatively sophisticated attempt to balance
consistency requirements with load on your infrastructure.

## Walking the keyspace

Inconsistent keys can only be repaired if they're read. To guard against long
term data corruption, the [roshi- walker][walker] component is designed to
continuously walk the keyspace to enforce data consistency.

[walker]: https://github.com/soundcloud/roshi/blob/master/roshi-walker
