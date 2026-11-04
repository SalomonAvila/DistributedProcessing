#let navy = rgb("#17365D")
#let pale = rgb("#EAF0F7")
#let dbfill = rgb("#EDF7ED")

#set document(
  title: "Preliminar de arquitectura: Procesamiento distribuido de base de datos SECOP II y RAG híbrido",
  author: "DistributedProcessing",
)
#set page(paper: "a4", margin: 1.65cm, numbering: "1")
#set text(font: "New Computer Modern", size: 10pt, lang: "es")
#set par(justify: true, spacing: 0.75em)
#set heading(numbering: "1.")
#show heading: set block(above: 1.1em, below: 0.6em)
#show heading.where(level: 1): set text(size: 12pt)
#show link: set text(fill: navy)
#set list(indent: 0.4em, body-indent: 0.5em, spacing: 0.45em)
#set enum(indent: 0.4em, body-indent: 0.5em, spacing: 0.45em)

// ---- Bloques del diagrama ----
#let nodebox(body) = box(
  width: 2.6cm,
  inset: 5pt,
  radius: 3pt,
  stroke: 1pt + navy,
  fill: pale,
  align(center, text(size: 7.5pt, body)),
)
#let db(body) = box(
  width: 2.6cm,
  inset: 5pt,
  radius: (top: 8pt, bottom: 8pt),
  stroke: 0.8pt + luma(110),
  fill: dbfill,
  align(center, text(size: 7.5pt, body)),
)
#let arr(sym) = text(fill: navy, size: 13pt, weight: "bold", sym)

#align(center)[
  #text(size: 15pt, weight: "bold")[
    Preliminar de arquitectura: Procesamiento distribuido \
    de base de datos SECOP II y RAG híbrido
  ]
  #v(0.2em)
  #text(size: 10pt)[Propuesta técnica del proyecto]
]

#v(0.6em)

= Propósito y alcance

El proyecto propone una plataforma para analizar grandes volúmenes de información de contratación pública de SECOP II mediante un motor de procesamiento distribuido propio y utilizar los resultados en un sistema de consulta en lenguaje natural.

Para que cualquier persona tenga acceso a la información de contratación pública en lenguaje natural, se busca calcular indicadores que permitan identificar *señales de riesgo* en los procesos de contratación y facilitar consultas estadísticas y explicativas.

Se trabajará principalmente con dos conjuntos de datos: _Procesos de Contratación_, que contiene información de participantes, invitados e interesados, y _Contratos Electrónicos_, que registra contratos adjudicados, proveedores ganadores, entidades, valores, categorías UNSPSC y modalidades.

La solución integra dos capacidades: (1) procesamiento paralelo mediante Go y MapReduce; y (2) un RAG híbrido que combina consultas SQL con búsqueda semántica. Los indicadores serán herramientas de priorización y revisión: por sí solos no constituyen prueba de corrupción o ilegalidad.

= Arquitectura y flujo general

#figure(
  {
    set align(center + horizon)
    grid(
      columns: 7,
      column-gutter: 4pt,
      row-gutter: 6pt,
      nodebox[SECOP II \ Procesos y \ contratos], arr[→],
      nodebox[Ingesta \ validación], arr[→],
      db[Data Lake \ CSV / Parquet], arr[→],
      nodebox[Motor Go \ MapReduce],

      [], [], [], [], [], [], arr[↓],

      nodebox[API e interfaz \ de consulta], arr[←],
      nodebox[RAG híbrido \ SQL + vectorial], arr[←],
      stack(spacing: 5pt, db[PostgreSQL \ datos analíticos], db[pgvector \ embeddings]), arr[←],
      nodebox[Indicadores \ y unión],
    )
  },
  caption: [Vista general de los componentes y del recorrido de los datos.],
)

El flujo comienza con la descarga y validación de SECOP II. Los datos originales y normalizados se conservan en un Data Lake; después se dividen en particiones para procesarlas en paralelo. El motor calcula indicadores y genera resultados estructurados. Estos resultados se cargan en PostgreSQL y, cuando contienen explicaciones o evidencia textual útil, también se convierten en documentos vectoriales en pgvector. Finalmente, el RAG selecciona la fuente adecuada para responder cada pregunta.

= Ingesta, almacenamiento y preparación

Un servicio de ingesta obtiene los conjuntos de datos, verifica columnas y tipos, normaliza fechas e identificadores, identifica duplicados y registra la fecha y versión de cada extracción. Se conservarán los archivos originales para trazabilidad y para poder repetir los análisis.

Se propone un Data Lake, por ejemplo MinIO compatible con S3, con tres zonas:

- *Raw:* archivos originales sin modificar.
- *Processed:* datos depurados y normalizados, preferiblemente en Parquet.
- *Results:* resultados intermedios y finales de las ejecuciones distribuidas.

Parquet facilita el procesamiento analítico por columnas. Un componente _Chunker_ divide los archivos en particiones manejables, que serán las unidades de trabajo asignadas a los Workers.

= Motor de procesamiento distribuido

El motor se implementará en Go con arquitectura *Master–Worker*, comunicación gRPC y mensajes definidos mediante Protocol Buffers. El Master coordina las tareas; los Workers procesan las particiones.

- *Master:* divide y asigna tareas, mantiene su estado, coordina el Shuffle, recibe resultados y detecta fallos mediante _heartbeats_.
- *Workers:* leen la partición asignada, ejecutan Map o Reduce, escriben resultados y reportan su estado.
- *Map:* transforma registros en pares clave–valor.
- *Shuffle:* agrupa y redistribuye los pares por clave para reunir registros relacionados.
- *Reduce:* procesa cada grupo y calcula agregaciones o indicadores.

Si un Worker deja de responder, el Master podrá reasignar sus tareas pendientes. Las salidas deben identificarse por ejecución y tarea para evitar contabilizar dos veces un resultado. Docker empaqueta los servicios y Kubernetes administra su despliegue y reinicio; Kubernetes no reemplaza al Master, pues este último controla las tareas MapReduce.

*Estado actual del repositorio:* ya están implementados el Chunker (indexación por offsets del CSV), la cola de tareas en el Master, los heartbeats con reasignación de tareas ante timeout, el Shuffle entre Workers, los Jobs A, B1 y B2 y el join de la Etapa 2, además de los manifiestos de Kubernetes y la publicación de imágenes en GHCR. Quedan pendientes: ejecutar el join como tarea distribuida (hoy se calcula dentro del Master), persistir los resultados y todos los componentes de este documento posteriores al motor (Data Lake, PostgreSQL, pgvector, RAG y API).

= Cálculo de indicadores y unión de resultados

El diseño contempla tres trabajos independientes:

+ *Job A – Competencia:* calcula la relación entre proveedores que respondieron y proveedores invitados:
  $ I_c = "proveedores que respondieron" / "proveedores invitados". $
  Debe controlar denominadores cero, valores faltantes y duplicados.

+ *Job B1 – Desviación de precios:* compara el valor de un contrato con la mediana de referencia de su categoría UNSPSC:
  $ D_p = (V_"contrato" - op("Mediana")(V_"categoría")) / (op("Mediana")(V_"categoría")) times 100. $
  La comparación debe considerar contratos suficientemente similares. No se deben promediar medianas locales de Workers para obtener la mediana global; se requiere un cálculo distribuido correcto o un método de cuantiles aproximados documentado.

+ *Job B2 – Concentración:* mide qué porcentaje de los contratos de una entidad se adjudicó a un proveedor:
  $ C_p = "contratos de la entidad adjudicados al proveedor" / "total de contratos de la entidad" times 100. $

Los resultados se relacionan mediante un *Reduce Side Join* usando el identificador del proceso. El Shuffle agrupa los registros de los distintos Jobs por esa clave y Reduce combina sus indicadores. Se propone una unión externa izquierda desde los procesos, para conservar también los que no tienen contrato adjudicado.

Opcionalmente, los indicadores pueden combinarse en un puntaje de priorización:
$ R = w_1 S_c + w_2 S_p + w_3 S_k, $
donde los indicadores se normalizan y los pesos $w_i$ se definen y justifican durante el proyecto. Los pesos y umbrales todavía no están establecidos. Se conservarán los valores individuales y sus fuentes, no solo el puntaje.

= Persistencia e índice vectorial

El motor escribirá sus resultados en Parquet. Un proceso _Data Loader_ validará los archivos y los cargará en una base analítica. Se propone comenzar con *PostgreSQL*, con tablas para procesos, contratos, proveedores, entidades, métricas de competencia, precios, concentración y alertas. Se conservarán todos los procesos y contratos, no únicamente los que generen alertas, para evitar sesgos en las consultas estadísticas.

La búsqueda vectorial complementa, pero no reemplaza, a SQL:

- *PostgreSQL:* conteos, sumas, filtros, agrupaciones, rankings y consultas por fecha.
- *pgvector:* búsqueda semántica de explicaciones, observaciones, metodología y fragmentos de documentos de soporte.

No es necesario generar embeddings para cada fila. Se construirán documentos textuales a partir de alertas y evidencias relevantes, se dividirán en fragmentos (_chunks_) y se transformarán en vectores mediante un modelo de embeddings, por ejemplo Sentence Transformers. Cada fragmento conservará metadatos y referencias al proceso o contrato de origen.

= RAG híbrido y acceso del usuario

El RAG híbrido decide cómo recuperar la información según la pregunta:

+ *Query Router:* clasifica la pregunta como estructurada, semántica o mixta.
+ *SQL Retriever:* consulta PostgreSQL para obtener cifras y agregaciones.
+ *Vector Retriever:* busca en pgvector explicaciones y documentos relacionados.
+ *Context Aggregator:* combina resultados y conserva sus referencias.
+ *LLM Generator:* redacta una respuesta basada en el contexto recuperado.

Por ejemplo, "¿qué empresa obtuvo más contratos en 2021?" requiere SQL. En cambio, "¿por qué esta empresa presenta señales de riesgo?" requiere consultar sus indicadores en SQL y recuperar explicaciones o evidencias relacionadas mediante búsqueda vectorial.

Se propone exponer el sistema mediante una API, por ejemplo FastAPI, que conecte la interfaz con el RAG. La aplicación no accederá directamente a las bases de datos. Las consultas SQL serán de solo lectura, con tablas permitidas, validación, límites y tiempos máximos. Las respuestas deberán mostrar cifras, periodo e identificadores de procesos o contratos cuando estén disponibles. Si no hay evidencia suficiente, el sistema deberá indicarlo y no inventar conclusiones.

= Operación, tecnologías y etapas

Airflow podrá coordinar las etapas periódicas de ingesta, preparación, ejecución MapReduce, carga SQL e indexación vectorial. No sustituye al Master: Airflow coordina el pipeline completo y el Master coordina las tareas internas del motor. Prometheus y Grafana permitirán monitorear duración, registros procesados, throughput, uso de CPU y memoria, fallos, reintentos y latencia del RAG.

#figure(
  {
    set text(size: 9pt)
    table(
      columns: (3.2cm, 1fr),
      align: left,
      stroke: none,
      inset: (x: 5pt, y: 4pt),
      table.hline(stroke: 1pt),
      table.header([*Componente*], [*Tecnología propuesta / función*]),
      table.hline(stroke: 0.5pt),
      [*Ingesta y preparación*], [Python; descarga, validación y normalización.],
      [*Almacenamiento*], [MinIO/S3 y Parquet; archivos originales, procesados y resultados.],
      [*Procesamiento*], [Go, MapReduce, gRPC y Protocol Buffers.],
      [*Despliegue*], [Docker y Kubernetes.],
      [*Persistencia*], [PostgreSQL para información estructurada.],
      [*Búsqueda semántica*], [pgvector y un modelo de embeddings.],
      [*RAG y API*], [Router, recuperadores SQL/vectoriales, LLM y FastAPI.],
      [*Operación*], [Airflow para orquestación; Prometheus y Grafana para monitoreo.],
      table.hline(stroke: 1pt),
    )
  },
  kind: table,
  caption: [Tecnologías propuestas por componente.],
)

*Plan de implementación:* (1) ingesta y almacenamiento; (2) motor MapReduce genérico y tolerancia a fallos; (3) Jobs A, B1, B2 y Reduce Side Join; (4) persistencia en PostgreSQL; (5) documentos y embeddings en pgvector; (6) RAG híbrido; (7) API e interfaz; (8) automatización y monitoreo.

*Resultado esperado:* una plataforma que procese SECOP II de forma distribuida, produzca indicadores trazables y permita consultar tanto estadísticas como explicaciones mediante lenguaje natural. La arquitectura es preliminar: el esquema físico definitivo, los pesos del ranking, los umbrales y las decisiones finales de infraestructura deberán validarse durante la implementación.
