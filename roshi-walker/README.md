# roshi-walker

roshi-walker walks the keyspace of a Roshi farm in semirandom order (random
order of Redis instances; Redis [SCAN][scan] command on each instance) and a
user-defined rate. It makes Select request for each key, using the
[SendAllReadAll read strategy][send-all-read-all] in order to perform complete
read repair.

[scan]: http://redis.io/commands/scan
[send-all-read-all]: https://github.com/soundcloud/roshi/tree/master/farm#read-strategies

## Getting and building

roshi-walker uses vendored dependencies and a "blessed build" process to ensure
stability over time. Users should get and build roshi-walker by cloning this
repository and running `make` in the roshi-walker subdirectory. A working Go
toolchain is assumed.

    git clone git@github.com:soundcloud/roshi
    cd roshi/roshi-walker
    make

It's also possible to get roshi-walker via `go get`, and/or build it with a
simple `go build`, with the caveat that it will use your normal GOPATH to
resolve dependencies, and therefore will enforce no constraints on dependency
versions, which could introduce bugs or strange behavior.

## Usage

roshi-walker is designed to be used in two situations.

### Walk forever

One or more roshi-walker instances can be started for a given Roshi farm, set
to walk at a moderate rate and run forever. That will ensure that all keys are
made consistent, within some upper time bound. That in turn improves data
durability, especially if normal reads are non-uniform or low volume.

It's recommended to run a few roshi-walker processes in this mode on every
Roshi farm. Test against your infrastructure to determine an appropriate rate.

### Walk once

roshi-walker supports a **-once** flag, which will walk the entire keyspace
once and exit. This is useful if a Redis instance has crashed and come back
online with no data. In this situation, while the farm still returns correct
data, it is less resilient to further node failure. After the walk is
complete, the empty instance will be repopulated with relevant data via read
repair, and the resiliency of the farm is returned to normal levels.
