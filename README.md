# Procesamiento distribuido de SECOP II y consulta en lenguaje natural mediante RAG híbrido

## Integrantes

- Salomon Avila
- Andrés Felipe Caro
- Lorenzo Ramirez Calderon
- Eliana Pardo
- Gabriela Bonilla
- Samuel Montaña

## Resumen

La contratación pública colombiana se publica de forma abierta en SECOP II, pero repartida en dos conjuntos de datos que no se cruzan entre sí: Procesos de Contratación, con más de 9 millones de registros, y Contratos Electrónicos. Por eso es difícil detectar señales de riesgo y consultar esa información sin conocimientos técnicos. Este proyecto construye desde cero, en Go, un motor de procesamiento distribuido basado en MapReduce, con arquitectura Master–Worker (un master y tres workers), comunicación gRPC y despliegue en Kubernetes. El motor calcula tres indicadores en paralelo: el índice de competencia real, la desviación de precio frente a la mediana de la categoría UNSPSC y la concentración proveedor–entidad. Luego los combina mediante un reduce-side join distribuido en un indicador conjunto de riesgo por proceso. El sistema tolera fallos a nivel de tarea, mediante heartbeats y reasignación de chunks, y a nivel de proceso, mediante Kubernetes. Su rendimiento se evalúa con speedup, eficiencia, throughput y tiempos de recuperación. Los resultados se cargan en una base analítica y se exponen mediante un RAG híbrido, que combina consultas SQL con búsqueda semántica, para que cualquier persona pueda hacer preguntas estadísticas y explicativas en lenguaje natural. Los indicadores son una herramienta de priorización para investigación posterior, no una prueba de irregularidad.

## Problema

SECOP II publica la contratación pública de Colombia en dos conjuntos de datos separados:

- **Procesos de Contratación** (unos 9,14 millones de filas, 59 columnas, con actualización diaria): registra el embudo de competencia de cada proceso, es decir, los proveedores invitados, los que manifestaron interés y los que efectivamente respondieron.
- **Contratos Electrónicos** (unos 6 millones de filas, 95 columnas): registra el contrato adjudicado, con el proveedor ganador, la entidad, el valor, la categoría UNSPSC y la modalidad.

No existe un indicador público que cruce ambas fuentes, y la información tampoco es fácil de consultar sin conocimientos técnicos. El proyecto ataca los dos problemas:

1. **Motor distribuido:** un motor MapReduce propio, escrito en Go, calcula a escala tres señales de riesgo y las une por proceso.
2. **Capa de consulta:** un RAG híbrido (SQL más búsqueda semántica) permite consultar en lenguaje natural tanto los indicadores como la información de contratación.

Los indicadores son una señal preliminar para priorizar revisiones. No sustituyen a los organismos de control ni prueban irregularidades.

## Indicadores

| Job | Agrupa por | Indicador |
|---|---|---|
| **A – Competencia** | `id_del_proceso` | $I_c = \dfrac{\text{proveedores únicos con respuesta}}{\text{proveedores invitados}}$ |
| **B1 – Precio** | categoría UNSPSC | $D_p = \dfrac{V_{\text{contrato}} - \text{Mediana}(V_{\text{categoría}})}{\text{Mediana}(V_{\text{categoría}})}$ |
| **B2 – Concentración** | (proveedor, entidad) | $C_p = \dfrac{\text{contratos del proveedor con la entidad}}{\text{contratos totales del proveedor}}$ |

Los tres jobs forman la **Etapa 1** y corren en paralelo, sin dependencias entre sí. La **Etapa 2** es un *reduce-side join* por identificador de proceso que combina los tres resultados en una puntuación de riesgo. Usa un *left outer join* para conservar los procesos sin contrato asociado (no adjudicados o desiertos).

## Arquitectura

```
SECOP II (CSV) ──► Chunker ──► Master ──gRPC──► Workers (Map → Shuffle → Reduce)
                                 │
                                 └──► Join (Etapa 2) ──► Resultados ──► PostgreSQL + pgvector ──► RAG híbrido ──► API
```

- **Master:** indexa los CSV por offsets de bytes (nunca carga el dataset completo en memoria), mantiene la cola de tareas, asigna chunks, detecta workers caídos por timeout de heartbeat, reasigna sus tareas y coordina el join.
- **Workers:** ejecutan Map y Reduce, intercambian pares clave–valor entre sí en el shuffle (`ReceiveShuffleData`) y reportan resultados y heartbeats al master.
- **Kubernetes:** solo gestiona el ciclo de vida de los procesos. El master corre como `Deployment` y los workers como `StatefulSet` con un Service headless, `podAntiAffinity` y probes. El reparto de trabajo y la tolerancia a fallos a nivel de tarea son responsabilidad del motor propio, que es el objeto de estudio del proyecto.
- **RAG híbrido (diseño):** un *router* clasifica cada pregunta como estructurada, semántica o mixta y la dirige a PostgreSQL (cifras, filtros, rankings), a pgvector (búsqueda semántica) o a ambos. Un LLM redacta la respuesta citando los identificadores de proceso y contrato. Las consultas SQL son de solo lectura y se limitan a tablas permitidas. Ver `docs/04-rag.typ`.

Hay dos mecanismos de recuperación ante fallos, que se miden por separado:

- **Recuperación de tarea:** el master reasigna el chunk de un worker caído. La meta es 15 s o menos.
- **Recuperación de proceso:** Kubernetes reprograma el pod según sus probes.

## Estado actual

**Implementado:**

- Contratos gRPC y Protocol Buffers completos (`src/proto/mapreduce.proto`): asignación de tareas, heartbeats, reporte de resultados y shuffle entre workers.
- Chunker con indexación por offsets y particionamiento en micro-chunks, con tamaño configurable mediante `MAP_CHUNK_SIZE`.
- Cola de tareas, pool de workers, monitor de heartbeats y reasignación ante timeout.
- Jobs A, B1 y B2 (Map y Reduce) y el join de la Etapa 2.
- Oráculo secuencial en Python (`scripts/reference_oracle.py`) con las salidas esperadas en `data/sample/expected/`.
- Generador de datos sintéticos (`src/cmd/gendata`).
- Dockerfile multi-etapa autocontenido, `docker-compose` para desarrollo local, manifiestos de Kubernetes y publicación automática de imágenes en GHCR mediante GitHub Actions.
- Tests unitarios del chunker, de los jobs, del join y del master (`make test`).

**Pendiente:**

- **Join distribuido:** hoy el join de la Etapa 2 se calcula dentro del master. Falta ejecutarlo como tarea MapReduce en los workers.
- **Persistencia de resultados:** el master solo registra en el log cuántos procesos evaluó. Falta escribir los resultados a disco (CSV/Parquet) y cargarlos en PostgreSQL.
- **Tercer worker en el clúster:** `k8s/worker-statefulset.yaml` corre temporalmente con 2 réplicas (nodos `worker2` y `worker3`). Hay que volver a 3 réplicas y agregar `worker4`.
- **Métricas:** falta instrumentar speedup, eficiencia, throughput, tráfico de shuffle y tiempos de recuperación.
- **Capa de consulta:** falta todo: esquema de PostgreSQL, carga de datos, embeddings en pgvector, RAG y API.

**Problemas conocidos:**

- **Riesgo de procesos sin contrato:** los procesos sin contrato reciben `0.0` en los indicadores ausentes y terminan con puntuación de riesgo alta (por ejemplo, `PROC-2026-009` sale con `1.0`). Hay que distinguir "dato ausente" de "valor cero" antes de exponer el ranking.
- **Nombre de entidad:** en el oráculo, `nombre_entidad` se llena con el NIT porque la muestra no trae el nombre.

## Estructura del repositorio

```
src/
  cmd/master/        Punto de entrada del master
  cmd/worker/        Punto de entrada del worker
  cmd/gendata/       Generador de datasets sintéticos
  pkg/chunker/       Indexación y particionamiento de los CSV
  pkg/master/        Coordinador, cola de tareas, pool de workers, heartbeats
  pkg/jobs/          Jobs competition (A), price (B1), concentration (B2) y join
  proto/             Definición gRPC / Protocol Buffers
scripts/             Oráculo secuencial de referencia y sus tests
data/                Muestras, datos sintéticos y destino de los datasets crudos (ver data/README.md)
k8s/                 Manifiestos y guía de despliegue (ver k8s/README.md)
docs/                Documentación formal en Typst y diagramas drawio
```

## Cómo ejecutar

Requisitos: Go 1.24 o superior y `protoc` (solo para `make proto`, `make build` y `make test`). Para el oráculo hace falta Python 3; para levantar el sistema completo, Docker.

```bash
make test        # genera el código proto y corre los tests de Go
make oracle      # regenera las salidas esperadas con el oráculo secuencial
make build       # compila bin/master y bin/worker
```

Para generar datos sintéticos con el mismo esquema que SECOP II:

```bash
cd src && go run ./cmd/gendata -out ../data/synthetic -procesos 30 -contratos 40 -seed 42
```

Para levantar el sistema local completo (un master y tres workers sobre `data/sample-real`):

```bash
docker compose up --build
```

`docker-compose.yaml` es solo para desarrollo: no tiene healthchecks, así que no sirve para validar la recuperación de procesos. El despliegue real en el clúster está documentado paso a paso en [`k8s/README.md`](k8s/README.md).

**Acceso de usuarios:** el único punto de entrada es el front (`k8s/front.yaml`), que se publica en el puerto 80 de `worker1` para la red de la universidad y reenvía `/api/` a la API interna. Todos los demás componentes son internos al clúster. Ver la sección "Acceso desde la red de la universidad" en `k8s/README.md`.

Los datasets completos se descargan de datos.gov.co y se ubican en `data/raw/`, que no se versiona. Ver [`data/README.md`](data/README.md).

### Variables de entorno principales

| Variable | Componente | Descripción |
|---|---|---|
| `MASTER_PORT` / `WORKER_PORT` | ambos | Puerto gRPC (por defecto `50051`) |
| `WORKERS` | ambos | Lista `host:puerto` de los workers |
| `MASTER_ADDRESS` | worker | Dirección del master |
| `HEARTBEAT_TIMEOUT_SECONDS` | master | Timeout para declarar caído un worker (por defecto 6) |
| `DATA_PROCESOS_PATH` / `DATA_CONTRATOS_PATH` | master | Rutas de los CSV |
| `MAP_CHUNK_SIZE` | master | Filas por tarea Map (por defecto 5; en el clúster, 50000) |

## Hardware del clúster

Son cuatro máquinas físicas idénticas conectadas por Ethernet Gigabit en una LAN cerrada: un nodo master y tres nodos worker. Cada una tiene 20 núcleos, 64 GB de RAM y 100 GB de almacenamiento.

## Métricas de éxito

- **Speedup:** mínimo de 2× con tres workers frente a la ejecución secuencial equivalente, medido por separado para la Etapa 1 y la Etapa 2.
- **Throughput:** agregado mínimo de 50 MB/s.
- **Recuperación de tarea:** 15 s o menos. La recuperación de proceso por Kubernetes se mide de forma independiente.
- **Corrección:** comparación exacta contra el oráculo secuencial sobre una muestra de datos, incluido el caso de procesos sin contrato.

El detalle está en `docs/01-alcance.typ`.

## Documentación

La documentación formal está en `docs/`, en formato [Typst](https://typst.app):

| Documento | Contenido |
|---|---|
| `00-propuesta.typ` | Objetivos, fuentes de datos y justificación |
| `01-alcance.typ` | Problema, no-objetivos, red, hardware, volumen de datos y métricas de éxito |
| `02-arquitectura.typ` | Decisiones técnicas, alternativas evaluadas y estrategia de particionamiento |
| `03-esquema-datos.typ` | Contratos de datos de entrada, intermedios y salida, y definición en Protocol Buffers |
| `04-rag.typ` | Arquitectura preliminar de la plataforma completa: persistencia, índice vectorial y RAG híbrido |

Los diagramas de componentes, despliegue, secuencia y del pipeline MapReduce están en `docs/*.drawio`. Los PDF se generan automáticamente en la carpeta `rendered/` en cada push a `main`, mediante el workflow `.github/workflows/render-docs.yml`. Para compilar uno localmente: `typst compile --root . docs/00-propuesta.typ rendered/00-propuesta.pdf`.
