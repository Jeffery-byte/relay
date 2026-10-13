.PHONY: test build run integration
test:
	go test -race ./...
	go vet ./...
build:
	go build -o bin/relay ./cmd/relay
run:
	docker compose up --build -d
integration:
	go test -race -tags integration ./internal/relay -count=1
