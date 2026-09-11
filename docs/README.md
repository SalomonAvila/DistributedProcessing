# 📚 Documentación - DistributedProcessing

Motor de procesamiento distribuido MapReduce para análisis de riesgo en contratación pública colombiana, construido desde cero en Go con gRPC, desplegado sobre Kubernetes.

El sistema procesa los conjuntos de datos abiertos de SECOP II (Procesos de Contratación y Contratos Electrónicos) para producir un indicador conjunto de riesgo que combina índice de competencia real, desviación de precio por categoría UNSPSC y concentración proveedor-entidad, mediante un pipeline de dos etapas con join distribuido (reduce-side join).

## 📋 Contenidos

### 📄 Documentación Técnica

Los documentos técnicos se escriben en [Typst](https://typst.app) (`.typ`) y se compilan automáticamente a PDF por un GitHub Action al hacer push a `main`, publicándose en `RenderedDocuments/` junto con los diagramas `.drawio` exportados a PNG.

- **`00-propuesta.typ`**: Propuesta formal del proyecto
  - Objetivo general y objetivos específicos
  - Fuentes de datos: Procesos de Contratación (9,14M filas) y Contratos Electrónicos
  - Justificación y contexto del análisis de riesgo en contratación pública

- **`01-alcance.typ`**: Alcance del proyecto
  - Problema que resuelve el sistema y no-objetivos
  - Restricciones de red y hardware disponible (4 nodos: 1 master + 3 workers, 20 cores / 64 GB RAM cada uno)
  - Volumen de datos esperado (20-40 GB) y métricas de éxito (speedup ≥ 2x, throughput ≥ 50 MB/s, recuperación de tarea ≤ 15s)

- **`02-arquitectura.typ`**: Decisiones técnicas y trade-offs
  - Justificación de Go, gRPC, Docker y Kubernetes con matrices de evaluación
  - Arquitectura Master-Worker: Deployment para el master, StatefulSet con Service headless para los workers, podAntiAffinity para un worker por nodo físico
  - Flujo del pipeline de dos etapas: Etapa 1 (Jobs A, B1, B2 independientes) → Etapa 2 (reduce-side join por `id_del_proceso`)

- **`03-esquema-datos.typ`**: Esquema de datos MapReduce
  - Entrada: `ProcessDataChunk` (procesos de contratación) y `ContractDataChunk` (contratos electrónicos)
  - Intermedios: `CompetitionMetrics` (Job A), `ContractPriceMetrics` (Reduce B1), `ProviderConcentration` (Reduce B2)
  - Salida: `RiskRecord` y `RiskAnalysisResult` con indicador de riesgo consolidado
  - Definición formal completa en Protocol Buffers v3

- **`05-pasos.md`**: Backlog priorizado (estilo Scrum)
  - Épicas 0-5 con historias de usuario, prioridad, estimación y Definition of Done
  - Orden de ejecución: deuda técnica → contratos proto → motor genérico → lógica de negocio → Kubernetes → métricas

### 📐 Diagramas Arquitectónicos

Todos los diagramas están en formato `.drawio` (compatible con [Draw.io](https://draw.io) y [Diagrams.net](https://app.diagrams.net)).

- **`DiagramaDeComponentes.drawio`**: Componentes del sistema y sus interacciones
- **`DiagramaDeDespliegue.drawio`**: Topología de despliegue sobre Kubernetes (master + 3 workers en nodos físicos distintos)
- **`DiagramaDeSecuencia.drawio`**: Flujos de comunicación gRPC entre master y workers
- **`ContratosMapReduce.drawio`**: Flujo de datos entre las fases Map, Shuffle y Reduce de ambas etapas

## 🏗️ Stack Tecnológico

```
┌──────────────────────────────────────────────────┐
│  Lenguaje: Go                                    │
├──────────────────────────────────────────────────┤
│  Protocolo: gRPC + Protocol Buffers v3           │
├──────────────────────────────────────────────────┤
│  Contenedorización: Docker (multi-stage, scratch)│
├──────────────────────────────────────────────────┤
│  Orquestación: Kubernetes (StatefulSet + headless│
│  Service para workers, Deployment para master)   │
└──────────────────────────────────────────────────┘
```

## ⚡ Quick Facts

| Aspecto | Descripción |
|---------|-------------|
| **Dominio** | Análisis de riesgo en contratación pública (SECOP II) |
| **Lenguaje** | Go (compilado, tipado estático, goroutines) |
| **Protocolo RPC** | gRPC (HTTP/2 + Protocol Buffers) |
| **Topología** | 1 master + 3 workers en nodos físicos distintos |
| **Contenedorización** | Docker (imágenes ~25 MB con `scratch`) |
| **Orquestación** | Kubernetes (StatefulSet, podAntiAffinity, probes) |
| **Pipeline** | Etapa 1: Jobs A, B1, B2 independientes → Etapa 2: reduce-side join |
| **Datos** | Procesos de Contratación (9,14M filas) + Contratos Electrónicos |

## 📝 Notas para Desarrolladores

- Los documentos técnicos están en formato Typst (`.typ`); no se versionan los PDF ni PNG generados a mano, esos se publican automáticamente en `RenderedDocuments/` vía GitHub Actions
- El `docker-compose.yaml` en la raíz del proyecto es **solo para desarrollo local** y no representa la topología real de producción (que es Kubernetes). No tiene healthchecks, por lo que no sirve para validar recuperación de procesos
- La generación de código gRPC a partir de `src/proto/mapreduce.proto` requiere `protoc` con los plugins `protoc-gen-go` y `protoc-gen-go-grpc`
- La documentación se actualiza conforme evoluciona la arquitectura

## 🔗 Enlaces Útiles

- [Go Documentation](https://golang.org/doc/)
- [gRPC Official Guide](https://grpc.io/docs/languages/go/)
- [Protocol Buffers](https://developers.google.com/protocol-buffers)
- [Docker Documentation](https://docs.docker.com/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Datos Abiertos SECOP II](https://www.datos.gov.co/)
- [Diagrams.net](https://app.diagrams.net/)

---

**Última actualización**: 10/09/2026
