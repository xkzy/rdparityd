.PHONY: fmt test run-sim run-journal run-allocate run-write run-read run-scrub run-api

fmt:
	gofmt -w ./cmd ./internal

test:
	go test ./...

run-sim:
	go run ./cmd/rtpctl simulate -disks 3 -extents 8 -extent-bytes 4096 -corrupt-disk 1 -corrupt-extent 2

run-journal:
	go run ./cmd/rtpctl journal-demo

run-allocate:
	go run ./cmd/rtpctl allocate-demo

run-write:
	go run ./cmd/rtpctl write-demo

run-read:
	go run ./cmd/rtpctl read-demo

run-scrub:
	go run ./cmd/rtpctl scrub-demo

run-api:
	go run ./cmd/rtparityd -listen :8080
