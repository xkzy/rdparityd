.PHONY: all build test fmt clean dist install uninstall release cross package deb

GO := go
GOOS := linux
GOARCH := amd64

VERSION := 0.1.0
REVISION := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILDDATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

DIST := dist
BINARIES := rtparityd rtpctl

PREFIX := /usr/local
BINDIR := $(PREFIX)/bin
LIBDIR := $(PREFIX)/lib/rtparityd
SYSDDIR := /etc
SHAREDIR := $(PREFIX)/share/rtparityd
MANPATH := $(PREFIX)/share/man

TARGETS := linux/amd64 linux/arm64 linux/armv7

all: test

fmt:
	gofmt -w ./cmd ./internal

test:
	go test ./...

build: build-local

build-local:
	@mkdir -p $(DIST)
	$(GO) build -ldflags="-s -w -X main.version=$(VERSION) -X main.revision=$(REVISION) -X main.builddate=$(BUILDDATE)" -o $(DIST)/rtparityd ./cmd/rtparityd
	$(GO) build -ldflags="-s -w -X main.version=$(VERSION) -X main.revision=$(REVISION) -X main.builddate=$(BUILDDATE)" -o $(DIST)/rtpctl ./cmd/rtpctl

$(TARGETS):
	@mkdir -p $(DIST)/$@
	GOOS=$(GOOS) GOARCH=$(shell echo $@ | cut -d/ -f2) $(GO) build -ldflags="-s -w -X main.version=$(VERSION) -X main.revision=$(REVISION) -X main.builddate=$(BUILDDATE)" -o $(DIST)/$@/rtparityd ./cmd/rtparityd
	GOOS=$(GOOS) GOARCH=$(shell echo $@ | cut -d/ -f2) $(GO) build -ldflags="-s -w -X main.version=$(VERSION) -X main.revision=$(REVISION) -X main.builddate=$(BUILDDATE)" -o $(DIST)/$@/rtpctl ./cmd/rtpctl

cross: $(TARGETS)

package:
	@mkdir -p $(DIST)/rtparityd-$(VERSION)-linux-amd64
	cp $(DIST)/rtparityd $(DIST)/rtparityd-$(VERSION)-linux-amd64/
	cp $(DIST)/rtpctl $(DIST)/rtparityd-$(VERSION)-linux-amd64/
	cp -r packaging/systemd $(DIST)/rtparityd-$(VERSION)-linux-amd64/
	cp -r packaging/scripts $(DIST)/rtparityd-$(VERSION)-linux-amd64/
	cp -r packaging/config $(DIST)/rtparityd-$(VERSION)-linux-amd64/
	cp -r docs/man $(DIST)/rtparityd-$(VERSION)-linux-amd64/
	cp README.md $(DIST)/rtparityd-$(VERSION)-linux-amd64/
	cp LICENSE $(DIST)/rtparityd-$(VERSION)-linux-amd64/
	cd $(DIST) && tar -czf rtparityd-$(VERSION)-linux-amd64.tar.gz rtparityd-$(VERSION)-linux-amd64/
	@echo "Package created: $(DIST)/rtparityd-$(VERSION)-linux-amd64.tar.gz"

clean:
	rm -rf $(DIST)/*

install: build
	@echo "Installing rtparityd to $(BINDIR)..."
	install -m 755 $(DIST)/rtparityd $(BINDIR)/
	install -m 755 $(DIST)/rtpctl $(BINDIR)/
	@echo "Installing systemd service..."
	install -m 644 packaging/systemd/rtparityd.service $(SYSDDIR)/systemd/
	@echo "Installing default config..."
	install -m 644 packaging/config/config.yaml $(SYSDDIR)/rtparityd/config.yaml
	@echo "Installing scripts..."
	install -m 755 packaging/scripts/rtparityd-init.sh $(LIBDIR/)/
	@echo "Creating runtime directories..."
	mkdir -p /var/lib/rtparityd /var/run/rtparityd
	@echo "Reloading systemd..."
	systemctl daemon-reload
	@echo "Installation complete. Run 'systemctl start rtparityd' to start."

uninstall:
	@echo "Removing rtparityd from $(BINDIR)..."
	-rm $(BINDIR)/rtparityd
	-rm $(BINDIR)/rtpctl
	@echo "Removing systemd service..."
	-rm $(SYSDDIR)/systemd/rtparityd.service
	@echo "Removing config..."
	-rm -rf $(SYSDDIR)/rtparityd
	@echo "Removing scripts..."
	-rm -rf $(LIBDIR)
	@echo "Reloading systemd..."
	systemctl daemon-reload
	@echo "Uninstall complete."

release: clean cross package
	@echo "Release artifacts in $(DIST)/:"
	ls -la $(DIST)/*.tar.gz

.PHONY: all build test fmt clean dist install uninstall release cross package