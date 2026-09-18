.PHONY: build static clean version

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT = $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE = $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
LDFLAGS = -s -w -X main.Version=$(VERSION) -X main.Commit=$(COMMIT) -X main.Date=$(DATE)

build:
	go build -ldflags="$(LDFLAGS)" -o axiom-agent ./cmd/axiom-agent/main.go

static:
	CGO_ENABLED=0 go build -ldflags="$(LDFLAGS)" -o axiom-agent-static ./cmd/axiom-agent/main.go

dist:
	/home/team/shared/build_agent.sh $(VERSION)

clean:
	rm -f axiom-agent axiom-agent-static
	rm -rf dist

version:
	@echo "Version: $(VERSION)"
	@echo "Commit:  $(COMMIT)"
	@echo "Date:    $(DATE)"
