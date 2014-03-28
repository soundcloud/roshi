GOPATH := $(CURDIR)/../_vendor:$(GOPATH)

all: build

build:
	go build

clean:
	go clean

check:
	@go list -f '{{join .Deps "\n"}}' | xargs go list -f '{{if not .Standard}}{{.ImportPath}} {{.Dir}}{{end}}' | column -t
