# roshi-server

roshi-server provides a thin, REST-ish HTTP interface to a [farm][farm]. All
the parameters for each conceptual layer are provided as commandline flags,
most with sensible defaults. roshi-server expects an existant set of Redis
instances (at least 1) to be running somewhere as the data layer. roshi-server
is effectively stateless, so you may run as many instances as required to
satisfy your load volume.

[farm]: http://github.com/soundcloud/roshi/blob/master/farm

## Getting

```
go get github.com/soundcloud/roshi/roshi-server
```

## Running

As a demo, start an instance of Redis on the standard port, and run

```
roshi-server -redis.instances=localhost:6379
```

## API

The server installs one handler on the root path. Operations are
differentiated by their HTTP verb. Keys and members must be base64 encoded.
For demo purposes, we'll use key `foo` (base64 `Zm9v`) and members `bar`
(base64 `YmFy`) and `baz` (base64 `YmF6`).

Note that write operations will claim success and return 200 as long as quorum
is achieved, even if the provided score was lower than what has already been
persisted and therefore the operation was actually a no-op.

### Insert

POST to `/`. Provide a request body with a JSON array of key-score-member
objects. No URL parameters are accepted.

```bash
$ cat insert.json
[{"key":"Zm9v", "score":1.05, "member":"YmFy"},
 {"key":"Zm9v", "score":1.99, "member":"YmF6"}]

$ curl -Ss -d@insert.json -XPOST 'http://localhost:6302' | jq .
{
  "duration": "377.863us",
  "inserted":2
}
```

### Select

GET to `/`. Provide a request body with a JSON-encoded array of key strings.
There are some URL parameters:

- **offset**, for pagination, default 0
- **limit**, for pagination, default 10
- **coalesce**, merge multiple keys into one response, default false

```bash
$ cat select.json
["Zm9v"]

$ curl -Ss -d@select.json -XGET 'http://localhost:6302' | jq .
{
  "records": {
    "foo": [
      {
        "member": "YmF6",
        "score": 1.99,
        "key": "Zm9v"
      },
      {
        "member": "YmFy",
        "score": 1.05,
        "key": "Zm9v"
      }
    ]
  },
  "offset": 0,
  "limit": 10,
  "keys": [
    "Zm9v"
  ],
  "duration": "238.204us"
}
```

### Delete

DELETE to `/`. Provide a request body with a JSON array of key-score-member
objects. No URL parameters are accepted.

```bash
$ cat delete.json
[{"key":"Zm9v", "score":2.01, "member":"YmF6"}]

$ curl -Ss -d@delete.json -XDELETE 'http://localhost:6302' | jq .
{
  "duration": "429.993us",
  "deleted": 1
}
```

## Integrating with your code

Golang clients should `import "github.com/soundcloud/roshi/common"` and
interact with (i.e. serialize and deserialize) `common.KeyScoreMember` tuples
directly. That type implements `json.Marshaler` such that base64 encoding and
decoding is transparent to the user.
