# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) mod
BINARY_NAME=redis_performance_analysis
BINARY_UNIX=$(BINARY_NAME)_x86_64
REDIS_PKG=redis_performance_analysis
BUILDTS := $(shell date '+%Y-%m-%d %H:%M:%S')
GITHASH := $(shell git rev-parse HEAD)
GITBRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GOVERSION := $(shell go version)
LDFLAGS = ''
LDFLAGS += -X "$(REDIS_PKG)/public.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(REDIS_PKG)/public.GitHash=$(GITHASH)"
LDFLAGS += -X "$(REDIS_PKG)/public.GitBranch=$(GITBRANCH)"
LDFLAGS += -X "$(REDIS_PKG)/public.GoVersion=$(GOVERSION)"
all: build
build:
	CGO_ENABLED=1 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(BINARY_NAME)
mod:
	$(GOCMD) mod tidy
clean:
	$(GOCLEAN)
	rm -f $(BINARY_UNIX)
	rm -f $(BINARY_NAME)
run:
	CGO_ENABLED=1 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w'  -o $(BINARY_NAME)
	./$(BINARY_NAME)

# Cross compilation
linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GOBUILD)  -ldflags '$(LDFLAGS) -s -w' -o $(BINARY_UNIX)
