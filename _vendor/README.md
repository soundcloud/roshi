# vendor

This directory holds vendored dependencies, managed via [git subtrees][0]. All
management commands should be run from the top level of the repository. The
blessed build procedure for the binaries (encoded in their Makefiles) makes use
of this directory; normal development will use whichever version(s) are in your
normal GOPATH.

[0]: http://blogs.atlassian.com/2013/05/alternatives-to-git-submodule-git-subtree

## Adding a new dependency

    git subtree add --prefix _vendor/src/github.com/foo/bar git@github.com:foo/bar master --squash

## Updating a dependency

    git subtree pull --prefix _vendor/src/github.com/foo/bar git@github.com:foo/bar master --squash

