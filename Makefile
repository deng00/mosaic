# Pure-Go olm/megolm via build tag — no cgo, no libolm system dep.
GO_TAGS := goolm
BIN := mosaic-bin

.PHONY: build run clean tidy testmsg readroom all

all: build testmsg readroom

# cgo is still required by mattn/go-sqlite3 (mautrix's crypto store backend),
# but we no longer link against libolm — goolm provides olm/megolm in pure Go.
build:
	go build -tags $(GO_TAGS) -o $(BIN) .

testmsg:
	go build -tags $(GO_TAGS) -o testmsg ./cmd/testmsg

readroom:
	go build -tags $(GO_TAGS) -o readroom ./cmd/readroom

run: build
	@if [ ! -f .env ]; then echo "make a .env first (copy config.example.env)"; exit 1; fi
	set -a; . ./.env; set +a; ./$(BIN)

tidy:
	go mod tidy

clean:
	rm -f $(BIN) testmsg readroom
	rm -rf data/
