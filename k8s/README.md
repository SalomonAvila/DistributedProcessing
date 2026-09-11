# Despliegue en Kubernetes

Manifiestos para el clúster real de 4 nodos físicos: `worker1` aloja el master, `worker2`/`worker3`/`worker4` alojan un worker cada uno (ver `docs/02-arquitectura.typ`). Las imágenes se publican en GitHub Container Registry (`ghcr.io`) — se hace `push` una sola vez desde donde vos trabajás (tu PC o el nodo master) y Kubernetes las distribuye solo a los 4 nodos. No hace falta entrar por ssh a ningún worker, ni para el primer deploy ni para los siguientes.

## 0. Prerrequisitos

- Confirmar los nombres exactos de nodo con `kubectl get nodes -o wide` — los manifiestos usan literalmente `worker1`, `worker2`, `worker3`, `worker4` como valor de `kubernetes.io/hostname`. Si no coinciden, hay que ajustar `nodeSelector`/`nodeAffinity` en los YAML.
- `worker1` es el control-plane (tiene el taint `node-role.kubernetes.io/control-plane:NoSchedule`). No hace falta sacarlo: `master-deployment.yaml` ya trae una `toleration` puntual para ese pod, así el resto del clúster sigue sin admitir carga arbitraria en el control-plane.
- Los 4 nodos necesitan salida a internet hacia `ghcr.io` (ya lo confirmaste). containerd no necesita ninguna configuración adicional para pullear de un registry público con TLS válido.

## 1. Build + push de las imágenes (una sola vez por cambio de código)

Todo esto se corre en UNA máquina (tu PC o el master) — nunca en los workers.

```bash
# Login una sola vez (token de GitHub con scope write:packages, en
# github.com/settings/tokens):
echo "$GHCR_TOKEN" | docker login ghcr.io -u salomonavila --password-stdin

# Desde la raíz del repo:
docker build -f Dockerfile --target master -t ghcr.io/salomonavila/distributedprocessing-master:latest .
docker build -f Dockerfile --target worker -t ghcr.io/salomonavila/distributedprocessing-worker:latest .
docker push ghcr.io/salomonavila/distributedprocessing-master:latest
docker push ghcr.io/salomonavila/distributedprocessing-worker:latest
```

Después del primer push, los paquetes quedan **privados** por defecto en GHCR. Hay dos formas de resolverlo (elegí una, ambas son de una sola vez, no por nodo):

- **Más simple: hacerlos públicos.** En GitHub → tu perfil → Packages → `distributedprocessing-master` / `distributedprocessing-worker` → Package settings → Change visibility → Public. Con esto, `imagePullPolicy: Always` en los manifiestos ya alcanza, sin tocar nada más.
- **Si preferís dejarlos privados:** crear un `imagePullSecret` una sola vez (es un objeto de Kubernetes, se crea con `kubectl` desde donde sea que tengas acceso al clúster — no requiere ssh a los nodos, el kubelet de cada nodo lo consulta vía la API):
  ```bash
  kubectl create secret docker-registry ghcr-pull-secret \
    --docker-server=ghcr.io \
    --docker-username=salomonavila \
    --docker-password="$GHCR_TOKEN" \
    -n distributed-processing
  ```
  y descomentar el bloque `imagePullSecrets` en `k8s/master-deployment.yaml` y `k8s/worker-statefulset.yaml`.

## 2. Aplicar los manifiestos (primera vez)

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/worker-statefulset.yaml
kubectl apply -f k8s/master-deployment.yaml
```

## 3. Verificar que funciona

```bash
# Los 3 workers deben quedar Running, uno por nodo (worker2/3/4), y el master en worker1
kubectl -n distributed-processing get pods -o wide

# Logs del master: conexión a los 3 workers y reparto de las 9 tareas sintéticas
kubectl -n distributed-processing logs deploy/master -f
```

En los logs del master se debe ver, en este orden:
- `[Master] Conexión establecida con worker-0...` (x3, una por worker)
- `[Master] Todos los workers registrados y listos en el WorkerPool.`
- `[Master] Demostracion exitosa de reparto de tareas: Total: 9, Completadas: 9, Fallidas: 0, En progreso: 0`

Para revisar un worker puntual:
```bash
kubectl -n distributed-processing logs worker-0
kubectl -n distributed-processing describe pod worker-0   # si quedó Pending, ver eventos de scheduling/afinidad
```

## 4. Redeploy tras un cambio de código

De acá en adelante, cada cambio de código es solo esto — nunca hace falta tocar los workers a mano:

```bash
docker build -f Dockerfile --target master -t ghcr.io/salomonavila/distributedprocessing-master:latest .
docker build -f Dockerfile --target worker -t ghcr.io/salomonavila/distributedprocessing-worker:latest .
docker push ghcr.io/salomonavila/distributedprocessing-master:latest
docker push ghcr.io/salomonavila/distributedprocessing-worker:latest
kubectl -n distributed-processing rollout restart deployment/master
kubectl -n distributed-processing rollout restart statefulset/worker
```
`imagePullPolicy: Always` obliga a cada nodo a volver a pullear la etiqueta `latest`, así que el `rollout restart` sí trae el código nuevo. Los 4 nodos lo hacen solos.

## Limpieza

```bash
kubectl delete namespace distributed-processing
```
