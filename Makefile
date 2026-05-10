# Pure-Go olm/megolm via build tag — no cgo for olm, no libolm system dep.
# (cgo is still on for sqlite via mattn/go-sqlite3)
GO_TAGS := goolm

.PHONY: install build clean tidy testmsg readroom

# Default: install to $GOBIN (or $GOPATH/bin) — keeps source tree clean.
install:
	go install -tags $(GO_TAGS) .

# Build into the source tree (only for one-off testing / debugging).
build:
	go build -tags $(GO_TAGS) -o mosaic .

testmsg:
	go install -tags $(GO_TAGS) ./cmd/testmsg

readroom:
	go install -tags $(GO_TAGS) ./cmd/readroom

tidy:
	go mod tidy

clean:
	rm -f mosaic mosaic-bin testmsg readroom
