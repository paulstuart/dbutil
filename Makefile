#APP=dcman
#BASE=/opt/dcman

SHELL := /bin/bash

#export GOROOT := /usr/local/go
#export GOPATH := k

#export PATH := bin:$(PATH)

export PATH := $(PATH):$(GOROOT)/bin:$(GOPATH)/bin

GOFLAGS ?= $(GOFLAGS:)

all: install test

path:
	@echo $$PATH

bench:
	@go test -run=^$$ -bench=. -benchmem 

build:
	@go build $(goflags) ./...

profile:
	@go test -v -coverprofile cover.out

html:
	@go tool cover -html cover.out

show:	profile html

cover:
	@go test -cover $(arg1)  $(goflags) ./...

# for building static distribution on Alpine Linux
# https://dominik.honnef.co/posts/2015/06/go-musl/#flavor-be-gone
compile:
	CC=/usr/bin/x86_64-alpine-linux-musl-gcc go build --ldflags '-linkmode external -extldflags "-static"'

install: build test
	@go install $(arg1)

test: 
	@go test $(arg1)

clean:
	@go clean $(GOFLAGS) -i ./...

get:
	go get -u

fresh: get build

.PHONY: all test clean build compile install fresh get cover profile html show

## EOF
