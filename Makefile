.PHONY: server build test unit e2e demo kafka clean

server:            ## single-node Temporal with `production` + `sandbox` namespaces
	./scripts/dev-server.sh

build:
	go build -o bin/ ./cmd/...

unit:              ## no server needed
	go test ./pkg/...

e2e:               ## needs `make server`
	go test ./test/ -count=1 -v

test: unit e2e

demo:              ## needs `make server`
	./scripts/demo.sh

kafka:             ## optional real Kafka; then export KAFKA_BROKERS=localhost:9092
	docker compose up -d kafka

clean:
	rm -rf bin .demo-logs
