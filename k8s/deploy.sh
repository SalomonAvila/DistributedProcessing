#!/usr/bin/env bash
# Aplica los manifiestos de k8s/ en orden y espera a que todo quede Ready.
# Correr desde donde sea que tengas acceso al clúster (kubectl configurado)
# — no requiere ssh a ningún nodo.
set -euo pipefail

NAMESPACE=distributed-processing
K8S_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Aplicando namespace"
kubectl apply -f "$K8S_DIR/namespace.yaml"

echo "==> Aplicando workers (Service headless + StatefulSet)"
kubectl apply -f "$K8S_DIR/worker-statefulset.yaml"

echo "==> Aplicando master (Deployment + Service)"
kubectl apply -f "$K8S_DIR/master-deployment.yaml"

echo "==> Esperando rollout de workers"
kubectl -n "$NAMESPACE" rollout status statefulset/worker --timeout=120s

echo "==> Esperando rollout del master"
kubectl -n "$NAMESPACE" rollout status deployment/master --timeout=120s

echo "==> Pods:"
kubectl -n "$NAMESPACE" get pods -o wide

