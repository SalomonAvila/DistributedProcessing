.PHONY: proto build clean test

# Generar código Go a partir de proto/mapreduce.proto
proto:
	cd src && protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/mapreduce.proto

# Compilar binarios de master y worker
build: proto
	cd src && CGO_ENABLED=0 go build -o ../bin/master ./cmd/master
	cd src && CGO_ENABLED=0 go build -o ../bin/worker ./cmd/worker

# Ejecutar tests
test:
	cd src && go test -v ./...

# Limpiar binarios generados
clean:
	rm -rf bin/
