#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.prod.yml"

usage() {
    echo "Uso: $0 master | worker 1 | worker 2 | worker 3"
}

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Error: no existe ${ENV_FILE}. Copia .env.example a .env y configura las IPs y puertos." >&2
    exit 1
fi

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
    usage >&2
    exit 1
fi

role="$1"
service=""

case "${role}" in
    master)
        if [[ "$#" -ne 1 ]]; then
            usage >&2
            exit 1
        fi
        service="master"
        echo "Desplegando MASTER en esta máquina..."
        ;;
    worker)
        if [[ "$#" -ne 2 || ! "$2" =~ ^[123]$ ]]; then
            usage >&2
            exit 1
        fi
        service="worker-${2}"
        echo "Desplegando WORKER ${2} en esta máquina..."
        ;;
    *)
        echo "Error: rol no válido: ${role}" >&2
        usage >&2
        exit 1
        ;;
esac

echo "Usando configuración ${ENV_FILE}"
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" up -d "${service}"
echo "Despliegue de ${service} completado."