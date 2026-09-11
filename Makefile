.PHONY: proto tools build clean test

# Versiones fijas de los plugins de protoc, iguales a las usadas en el
# Dockerfile, para que `make proto` genere exactamente el mismo código
# sin importar qué haya instalado cada máquina.
PROTOC_GEN_GO_VERSION := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1

# Instala los plugins de protoc en $(go env GOPATH)/bin. Requiere tener
# `protoc` (el compilador de protobuf) disponible en el PATH; eso sí
# depende del sistema operativo (ej. apt install protobuf-compiler /
# brew install protobuf) y no se puede fijar por versión desde acá.
tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

# Generar código Go a partir de proto/mapreduce.proto
proto: tools
	cd src && PATH="$$(go env GOPATH)/bin:$$PATH" protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/mapreduce.proto

# Compilar binarios de master y worker
build: proto
	cd src && CGO_ENABLED=0 go build -o ../bin/master ./cmd/master
	cd src && CGO_ENABLED=0 go build -o ../bin/worker ./cmd/worker

# Ejecutar tests
test: proto
	cd src && go test -v ./...

# Limpiar binarios generados
clean:
	rm -rf bin/
