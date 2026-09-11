# Build autocontenido: no depende de nada preinstalado en el host (ni
# protoc, ni protoc-gen-go, ni *.pb.go generados a mano). Todo el
# toolchain se instala y fija por versión dentro de esta imagen, para
# que el resultado sea el mismo en cualquier máquina: laptop de
# desarrollo o nodo del clúster de producción.

# ---- Etapa 1: generación de código a partir de proto/mapreduce.proto ----
FROM golang:1.24 AS protogen

RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

ENV GOTOOLCHAIN=auto

# Versiones fijas (deben coincidir con las de src/go.mod) para que la
# generación sea reproducible y no dependa de qué versión tenga
# instalada cada máquina.
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

WORKDIR /app
COPY src/proto ./proto
RUN PATH="$(go env GOPATH)/bin:$PATH" protoc \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/mapreduce.proto

# ---- Etapa 2: build compartido (descarga de módulos + código fuente) ----
FROM golang:1.24 AS builder

ENV GOTOOLCHAIN=auto
WORKDIR /app

COPY src/go.mod src/go.sum ./
RUN go mod download

COPY src/ ./
COPY --from=protogen /app/proto/*.pb.go ./proto/

# ---- Binario del master ----
FROM builder AS build-master
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/master ./cmd/master

FROM scratch AS master
COPY --from=build-master /out/master /master
# Datasets sintéticos (mismo esquema de columnas que los CSV reales de
# SECOP II, ver data/README.md) para poder correr el pipeline completo
# (3 jobs + join) sin depender todavía de los datasets reales de 20-40GB.
# Regenerar con: go run ./cmd/gendata -out ../data/synthetic (desde src/).
COPY data/raw/prueba.csv /data/procesos-de-contratacion.csv
COPY data/raw/prueba.csv /data/contratos-electronicos.csv
EXPOSE 50051
CMD ["/master"]

# ---- Binario del worker ----
FROM builder AS build-worker
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/worker ./cmd/worker

FROM scratch AS worker
COPY --from=build-worker /out/worker /worker
EXPOSE 50051
CMD ["/worker"]
