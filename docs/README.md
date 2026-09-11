# Documentacion - DistributedProcessing

Motor de procesamiento distribuido MapReduce para analisis de riesgo en contratacion publica colombiana, construido desde cero en Go con gRPC, desplegado sobre Kubernetes.

El sistema procesa los conjuntos de datos abiertos de SECOP II (Procesos de Contratacion y Contratos Electronicos) para producir un indicador conjunto de riesgo que combina indice de competencia real, desviacion de precio por categoria UNSPSC y concentracion proveedor-entidad, mediante un pipeline de dos etapas con join distribuido (reduce-side join).

## Contenidos

### Documentacion Tecnica

Los documentos tecnicos se escriben en [Typst](https://typst.app) (`.typ`) y se compilan automaticamente a PDF por un GitHub Action al hacer push a `main`, publicandose en `RenderedDocuments/` junto con los diagramas `.drawio` exportados a PNG.

- **`00-propuesta.typ`**: Propuesta formal del proyecto
  - Objetivo general y objetivos especificos
  - Fuentes de datos: Procesos de Contratacion (9,14M filas) y Contratos Electronicos
  - Justificacion y contexto del analisis de riesgo en contratacion publica

- **`01-alcance.typ`**: Alcance del proyecto
  - Problema que resuelve el sistema y no-objetivos
  - Restricciones de red y hardware disponible (4 nodos: 1 master + 3 workers, 20 cores / 64 GB RAM cada uno)
  - Volumen de datos esperado (20-40 GB) y metricas de exito (speedup >= 2x, throughput >= 50 MB/s, recuperacion de tarea <= 15s)

- **`02-arquitectura.typ`**: Decisiones tecnicas y trade-offs
  - Justificacion de Go, gRPC, Docker y Kubernetes con matrices de evaluacion
  - Arquitectura Master-Worker: Deployment para el master, StatefulSet con Service headless para los workers, podAntiAffinity para un worker por nodo fisico
  - Flujo del pipeline de dos etapas: Etapa 1 (Jobs A, B1, B2 independientes) -> Etapa 2 (reduce-side join por `id_del_proceso`)
  - Estrategia de particionamiento dinámico (Chunker y Work Queue)

- **`03-esquema-datos.typ`**: Esquema de datos MapReduce
  - Entrada: `ProcessDataChunk` (procesos de contratacion) y `ContractDataChunk` (contratos electronicos)
  - Intermedios: `CompetitionMetrics` (Job A), `ContractPriceMetrics` (Reduce B1), `ProviderConcentration` (Reduce B2)
  - Salida: `RiskRecord` y `RiskAnalysisResult` con indicador de riesgo consolidado
  - Definicion formal completa en Protocol Buffers v3

- **`05-pasos.md`**: Backlog priorizado (estilo Scrum)
  - Epicas 0-5 con historias de usuario, prioridad, estimacion y Definition of Done
  - Orden de ejecucion: deuda tecnica -> contratos proto -> motor generico -> logica de negocio -> Kubernetes -> metricas

### Diagramas Arquitectonicos

Todos los diagramas estan en formato `.drawio` (compatible con [Draw.io](https://draw.io) y [Diagrams.net](https://app.diagrams.net)).

- **`DiagramaDeComponentes.drawio`**: Componentes del sistema y sus interacciones
- **`DiagramaDeDespliegue.drawio`**: Topologia de despliegue sobre Kubernetes (master + 3 workers en nodos fisicos distintos)
- **`DiagramaDeSecuencia.drawio`**: Flujos de comunicacion gRPC entre master y workers
- **`ContratosMapReduce.drawio`**: Flujo de datos entre las fases Map, Shuffle y Reduce de ambas etapas

## Stack Tecnologico

```
┌──────────────────────────────────────────────────┐
│  Lenguaje: Go                                    │
├──────────────────────────────────────────────────┤
│  Protocolo: gRPC + Protocol Buffers v3           │
├──────────────────────────────────────────────────┤
│  Contenedorizacion: Docker (multi-stage, scratch)│
├──────────────────────────────────────────────────┤
│  Orquestacion: Kubernetes (StatefulSet + headless│
│  Service para workers, Deployment para master)   │
└──────────────────────────────────────────────────┘
```

## Quick Facts

| Aspecto | Descripcion |
|---------|-------------|
| **Dominio** | Analisis de riesgo en contratacion publica (SECOP II) |
| **Lenguaje** | Go (compilado, tipado estatico, goroutines) |
| **Protocolo RPC** | gRPC (HTTP/2 + Protocol Buffers) |
| **Topologia** | 1 master + 3 workers en nodos fisicos distintos |
| **Contenedorizacion** | Docker (imagenes ~25 MB con `scratch`) |
| **Orquestacion** | Kubernetes (StatefulSet, podAntiAffinity, probes) |
| **Pipeline** | Etapa 1: Jobs A, B1, B2 independientes -> Etapa 2: reduce-side join |
| **Datos** | Procesos de Contratacion (9,14M filas) + Contratos Electronicos |

## Notas para Desarrolladores

- Los documentos tecnicos estan en formato Typst (`.typ`); no se versionan los PDF ni PNG generados a mano, esos se publican automaticamente en `RenderedDocuments/` via GitHub Actions
- El `docker-compose.yaml` en la raiz del proyecto es **solo para desarrollo local** y no representa la topologia real de produccion (que es Kubernetes). No tiene healthchecks, por lo que no sirve para validar recuperacion de procesos
- La generacion de codigo gRPC a partir de `src/proto/mapreduce.proto` requiere `protoc` con los plugins `protoc-gen-go` y `protoc-gen-go-grpc`
- La documentacion se actualiza conforme evoluciona la arquitectura

## Enlaces Utiles

- [Go Documentation](https://golang.org/doc/)
- [gRPC Official Guide](https://grpc.io/docs/languages/go/)
- [Protocol Buffers](https://developers.google.com/protocol-buffers)
- [Docker Documentation](https://docs.docker.com/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Datos Abiertos SECOP II](https://www.datos.gov.co/)
- [Diagrams.net](https://app.diagrams.net/)

---

**Ultima actualizacion**: 10/09/2026
