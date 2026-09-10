# Despliegue en producción

Este despliegue ejecuta un contenedor por máquina física: un `master` y tres
workers. Todos usan `network_mode: host`, por lo que cada proceso escucha
directamente en la red de su máquina.

## Requisitos previos

- Docker Engine y Docker Compose instalados en las cuatro máquinas.
- Acceso al repositorio y permisos para ejecutar Docker.
- Conectividad entre las cuatro máquinas.
- `MASTER_PORT` abierto para conexiones desde los workers hacia el master.
- `WORKER_PORT` abierto si el master inicia conexiones hacia los workers.
- El código fuente y los Dockerfiles deben estar presentes en cada máquina.

## Configuración común

En cada máquina, desde la raíz del repositorio:

```bash
cp deploy/.env.example deploy/.env
```

Edita `deploy/.env` y configura las mismas IPs y puertos reales en las cuatro
máquinas. No subas ese archivo al repositorio.

Antes del primer despliegue, haz ejecutable el script:

```bash
chmod +x deploy/deploy.sh
```

## Máquina MASTER

```bash
cd /ruta/al/DistributedProcessing
./deploy/deploy.sh master
```

Ver los logs:

```bash
docker logs -f master
```

## Máquinas WORKER

En la máquina del worker 1 ejecuta:

```bash
cd /ruta/al/DistributedProcessing
./deploy/deploy.sh worker 1
docker logs -f worker-1
```

En la máquina del worker 2 ejecuta:

```bash
cd /ruta/al/DistributedProcessing
./deploy/deploy.sh worker 2
docker logs -f worker-2
```

En la máquina del worker 3 ejecuta:

```bash
cd /ruta/al/DistributedProcessing
./deploy/deploy.sh worker 3
docker logs -f worker-3
```

El script levanta únicamente el servicio indicado; no debe ejecutarse el
compose completo en ninguna máquina.

## Verificación

Comprueba que el contenedor está ejecutándose en cada host:

```bash
docker compose --env-file deploy/.env -f deploy/docker-compose.prod.yml ps
```

En el master, revisa el registro de conexiones:

```bash
docker logs master
```

Debe aparecer el registro de los workers 1, 2 y 3 conectados, según el
formato de logs implementado por la aplicación. También puedes comprobar la
conectividad TCP desde el master con:

```bash
set -a
source deploy/.env
set +a
nc -vz "$WORKER_1_IP" "$WORKER_PORT"
nc -vz "$WORKER_2_IP" "$WORKER_PORT"
nc -vz "$WORKER_3_IP" "$WORKER_PORT"
```

## Troubleshooting

- Si falta `.env`, copia `.env.example` y configura sus valores en las cuatro
  máquinas.
- Si un worker no conecta, confirma que `MASTER_IP` coincide con la IP real del
  master y que `MASTER_PORT` es el mismo en todos los archivos `.env`.
- Verifica el firewall del master y que el puerto del master esté abierto para
  las IPs de los workers.
- Si el master no alcanza un worker, revisa `WORKER_n_IP`, `WORKER_PORT`, el
  firewall del worker y la salida de `docker logs worker-n`.
- Comprueba que ningún proceso del host ya esté usando el puerto configurado:

  ```bash
  sudo ss -ltnp | grep 50051
  ```

- Para reconstruir la imagen después de actualizar el código:

  ```bash
  docker compose --env-file deploy/.env -f deploy/docker-compose.prod.yml build --no-cache master
  docker compose --env-file deploy/.env -f deploy/docker-compose.prod.yml up -d master
  ```

  Sustituye `master` por `worker-1`, `worker-2` o `worker-3` según corresponda.

## Nota sobre el estado actual del código

Los Dockerfiles existentes compilan `src/master.go` y `src/builder.go`, pero el
estado actual del repositorio contiene `src/main.go`. El despliegue no modifica
esa situación: el build de Docker requerirá que esos archivos de entrada
existan y que implementen el contrato de variables descrito arriba.