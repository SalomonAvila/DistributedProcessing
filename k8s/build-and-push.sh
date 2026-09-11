#!/usr/bin/env bash
# Construye y publica en GHCR las imágenes de master y worker.
# Pensado para correr a mano (con `docker login ghcr.io` ya hecho, o
# GHCR_TOKEN en el entorno) o desde CI (ver
# .github/workflows/build-push-images.yml, que hace lo mismo pero con
# docker/build-push-action en vez de este script).
#
# Uso:
#   ./k8s/build-and-push.sh                # tag "latest" únicamente
#   IMAGE_TAG=v1.2.3 ./k8s/build-and-push.sh   # además publica ese tag
set -euo pipefail

REGISTRY="ghcr.io/salomonavila"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Tag adicional al de "latest": por default el short SHA del commit actual
# (si estamos en un repo git), para poder rastrear qué código quedó en cada
# imagen. Se puede pisar con IMAGE_TAG=lo-que-sea.
EXTRA_TAG="${IMAGE_TAG:-}"
if [ -z "$EXTRA_TAG" ] && git -C "$ROOT_DIR" rev-parse --short HEAD >/dev/null 2>&1; then
  EXTRA_TAG="$(git -C "$ROOT_DIR" rev-parse --short HEAD)"
fi

if [ -n "${GHCR_TOKEN:-}" ]; then
  echo "==> Login a ghcr.io con GHCR_TOKEN"
  echo "$GHCR_TOKEN" | docker login ghcr.io -u "${GHCR_USER:-salomonavila}" --password-stdin
fi

for target in master worker; do
  image_base="$REGISTRY/distributedprocessing-$target"

  echo "==> Build $image_base:latest (target=$target)"
  docker build -f "$ROOT_DIR/Dockerfile" --target "$target" -t "$image_base:latest" "$ROOT_DIR"

  if [ -n "$EXTRA_TAG" ]; then
    docker tag "$image_base:latest" "$image_base:$EXTRA_TAG"
  fi

  echo "==> Push $image_base:latest"
  docker push "$image_base:latest"

  if [ -n "$EXTRA_TAG" ]; then
    echo "==> Push $image_base:$EXTRA_TAG"
    docker push "$image_base:$EXTRA_TAG"
  fi
done

echo "==> Listo: distributedprocessing-{master,worker}:latest${EXTRA_TAG:+ y :$EXTRA_TAG} publicadas en $REGISTRY"
