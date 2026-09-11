#set document(
  title: "03 - Esquema de Datos MapReduce",
  author: "DistributedProcessing",
)
#set page(
  paper: "us-letter",
  margin: (x: 2.5cm, y: 2.5cm),
  numbering: "1",
)
#set text(font: "Liberation Serif", size: 11pt, lang: "es")
#set heading(numbering: "1.")
#set par(justify: true)

#show heading.where(level: 1): it => {
  v(1em)
  text(size: 16pt, weight: "bold")[#it]
  v(0.3em)
}

#align(center)[
  #text(size: 20pt, weight: "bold")[Esquema de Datos MapReduce]

  #text(size: 13pt)[Contratos de datos del sistema de análisis de riesgo en contratación pública]

  #v(0.3em)
  #text(size: 10pt, fill: gray)[DistributedProcessing, Documento 03-esquema-datos]
]

#v(1.5em)

Este documento define los contratos y estructuras de datos para las diferentes fases del ciclo de vida de procesamiento distribuido MapReduce en dos etapas: la Etapa 1 de agregación independiente por conjunto de datos, y la Etapa 2 de join distribuido sobre los resultados consolidados.

= Etapa 1: Entrada de Map por conjunto de datos

== Estructura del chunk para Procesos de Contratación

Cada Worker recibe un fragmento del conjunto de Procesos de Contratación de SECOP II.

Estos nombres de campo son los del modelo interno del sistema, no siempre coinciden con el fieldName real de la columna en el CSV publicado en datos.gov.co (dataset `p6dx-8zbt`). Diferencias conocidas: la columna de nombre de entidad se llama `entidad` (no `nombre_entidad`, como sí se llama en Contratos Electrónicos), y Socrata trunca el fieldName de proveedores únicos con respuesta a `proveedores_unicos_con`.

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  [`chunk_id`], [`string`], [Identificador único del bloque, por ejemplo process_chunk_001],
  [`source_dataset`], [`string`], [Identifica Procesos de Contratación como origen],
  [`id_del_proceso`], [`string`], [Identificador del proceso de compra en SECOP II],
  [`nit_entidad`], [`string`], [NIT de la entidad que publicó el proceso],
  [`nombre_entidad`], [`string`], [Nombre de la entidad, viene de la columna real `entidad`],
  [`proveedores_invitados`], [`uint32`], [Número de proveedores invitados a participar],
  [`proveedores_unicos_con_respuestas`], [`uint32`], [Proveedores únicos que presentaron respuesta, viene de la columna real `proveedores_unicos_con`],
  [`modalidad_de_contratacion`], [`string`], [Modalidad de selección del proceso],
  [`estado_del_procedimiento`], [`string`], [Estado actual del proceso],
)

== Estructura del chunk para Contratos Electrónicos

Cada Worker recibe un fragmento del conjunto de Contratos Electrónicos de SECOP II.

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  [`chunk_id`], [`string`], [Identificador único del bloque, por ejemplo contract_chunk_001],
  [`source_dataset`], [`string`], [Identifica Contratos Electrónicos como origen],
  [`proceso_de_compra`], [`string`], [Identificador del proceso asociado a este contrato],
  [`nit_entidad`], [`string`], [NIT de la entidad contratante],
  [`nombre_entidad`], [`string`], [Nombre de la entidad contratante],
  [`documento_proveedor`], [`string`], [NIT o cédula del proveedor adjudicado],
  [`proveedor_adjudicado`], [`string`], [Nombre del proveedor adjudicado],
  [`valor_del_contrato`], [`uint64`], [Valor total del contrato en pesos],
  [`codigo_de_categoria_principal`], [`string`], [Código UNSPSC de la categoría del bien/servicio],
  [`modalidad_de_contratacion`], [`string`], [Modalidad de contratación utilizada],
  [`fecha_de_firma`], [`string`], [Fecha de firma del contrato],
)

= Etapa 1: Salida de Map

La función Map de cada Job emite pares clave-valor intermedios según el conjunto procesado.

== Output del Job A: Índice de competencia por proceso

Para cada proceso, calcula y emite el índice de competencia real.

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Componente*], [*Tipo*], [*Descripción*]),
  [Clave], [`string`], [id_del_proceso],
  [Valor tipo], [`CompetitionMetrics`], [Estructura con índice de competencia],
)

Estructura CompetitionMetrics:

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  [`id_del_proceso`], [`string`], [Identificador del proceso],
  [`nit_entidad`], [`string`], [NIT de la entidad],
  [`proveedores_invitados`], [`uint32`], [Número de proveedores invitados],
  [`proveedores_responden`], [`uint32`], [Número de proveedores que respondieron],
  [`indice_competencia`], [`float32`], [Razón entre responden e invitados],
  [`modalidad_de_contratacion`], [`string`], [Modalidad del proceso],
)

== Output del Job B: dos reduces independientes sobre Contratos Electrónicos

El Job B calcula dos métricas de naturaleza distinta que requieren dos claves de agrupación diferentes, y por tanto se implementan como dos reduces separados dentro del mismo Job, no como uno solo. La desviación de precio es una propiedad de un contrato individual frente a su categoría; la concentración proveedor-entidad es una propiedad agregada de todos los contratos de un proveedor con una entidad, y no puede calcularse correctamente agrupando por proceso.

=== Reduce B1: desviación de precio, agrupado por categoría

La fase Map emite pares con clave categoria_unspsc y valor el contrato individual. El shuffle agrupa todos los contratos de la misma categoría en el mismo worker. El Reduce calcula la mediana de valor_del_contrato dentro de esa categoría y produce, por cada contrato, su desviación respecto a esa mediana.

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Componente*], [*Tipo*], [*Descripción*]),
  [Clave], [`string`], [codigo_de_categoria_principal],
  [Valor tipo], [`ContractPriceMetrics`], [Estructura con desviación de precio por contrato],
)

Estructura ContractPriceMetrics:

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  [`proceso_de_compra`], [`string`], [Identificador del proceso asociado],
  [`nit_entidad`], [`string`], [NIT de la entidad contratante],
  [`documento_proveedor`], [`string`], [NIT del proveedor adjudicado],
  [`valor_del_contrato`], [`uint64`], [Valor del contrato en pesos],
  [`categoria_unspsc`], [`string`], [Código de categoría UNSPSC],
  [`mediana_categoria`], [`uint64`], [Mediana de valor en la categoría],
  [`desviacion_precio`], [`float32`], [Desviación respecto a la mediana de categoría],
)

=== Reduce B2: concentración proveedor-entidad, agrupado por relación

La fase Map emite pares con clave compuesta documento_proveedor más nit_entidad, y valor el contrato individual. El shuffle agrupa todos los contratos de un mismo proveedor con una misma entidad en el mismo worker. El Reduce cuenta el total de contratos de ese proveedor en todas las entidades, el total de contratos de ese proveedor con esa entidad específica, y calcula el porcentaje de concentración.

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Componente*], [*Tipo*], [*Descripción*]),
  [Clave], [`string`], [documento_proveedor + nit_entidad],
  [Valor tipo], [`ProviderConcentration`], [Estructura con concentración por relación],
)

Estructura ProviderConcentration:

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  [`documento_proveedor`], [`string`], [NIT del proveedor],
  [`nit_entidad`], [`string`], [NIT de la entidad],
  [`contratos_con_entidad`], [`uint32`], [Contratos de este proveedor con esta entidad],
  [`contratos_totales_proveedor`], [`uint32`], [Contratos totales del proveedor en todas las entidades],
  [`concentracion_proveedor`], [`float32`], [Razón entre contratos_con_entidad y contratos_totales_proveedor],
)

El resultado de B2 se une de vuelta a nivel de proceso en la Etapa 2, junto con el resultado de B1, ya que ambos comparten documento_proveedor y nit_entidad como llave de referencia hacia cada proceso individual.

= Etapa 1: Reduce

El Reduce consolida las salidas intermedias por clave.

== Reduce del Job A

Recibe todas las CompetitionMetrics agrupadas por id_del_proceso y produce un registro por proceso.

== Reduce B1 del Job B

Recibe todas las ContractPriceMetrics agrupadas por categoria_unspsc, calcula la mediana de la categoría, y produce un registro por contrato con su desviación de precio.

== Reduce B2 del Job B

Recibe todos los contratos agrupados por documento_proveedor más nit_entidad y produce un registro por relación proveedor-entidad con su índice de concentración.

= Etapa 2: Join distribuido

El reduce-side join une los resultados consolidados de los tres reduces de la Etapa 1 sobre la clave id_del_proceso, siguiendo el patrón de etiquetado por origen.

== Mecánica del join

La fase Map de la Etapa 2 lee las tres tablas intermedias producidas en la Etapa 1 y etiqueta cada registro según su origen antes de emitirlo, produciendo pares con clave id_del_proceso y valor una tupla de origen más datos.

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Origen*], [*Tabla fuente*], [*Contenido emitido*]),
  [`A`], [Reduce Job A], [CompetitionMetrics, con id_del_proceso como clave directa],
  [`B1`], [Reduce B1], [ContractPriceMetrics, con proceso_de_compra como clave],
  [`B2`], [Reduce B2], [ProviderConcentration, referenciado por proceso a través de documento_proveedor y nit_entidad presentes en el registro B1 correspondiente],
)

El shuffle agrupa por id_del_proceso, de forma que todos los registros etiquetados A, B1 y B2 correspondientes al mismo proceso llegan al mismo worker de Reduce. El Reduce separa los valores recibidos según su etiqueta de origen y combina los tres en un único registro de salida.

Se maneja explícitamente el caso de un proceso presente en la tabla A pero sin registro correspondiente en B1 o B2, correspondiente a un proceso no adjudicado o desierto, como un left outer join: dicho registro se conserva en la salida con los campos de contrato marcados como no disponibles, en lugar de descartarse, ya que constituye un hallazgo propio de interés para el análisis de riesgo.

== Entrada del Join

Tabla intermedia A: CompetitionMetrics por proceso, resultado del Reduce Job A.

Tabla intermedia B1: ContractPriceMetrics por contrato, resultado del Reduce B1.

Tabla intermedia B2: ProviderConcentration por relación proveedor-entidad, resultado del Reduce B2.

Clave de unión principal: id_del_proceso en Tabla A equivalente a proceso_de_compra en Tabla B1. La Tabla B2 se referencia a través de la pareja documento_proveedor y nit_entidad presente en cada registro de B1.

== Salida del Join: Indicador de Riesgo Consolidado

Los campos relativos al contrato son opcionales y solo se completan cuando existe un registro correspondiente en la Tabla B1, según lo descrito para el left outer join. Un proceso sin contrato asociado conserva sus campos de competencia y se marca mediante tiene_contrato en falso.

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  [`id_del_proceso`], [`string`], [Identificador único del proceso],
  [`nit_entidad`], [`string`], [NIT de la entidad],
  [`indice_competencia`], [`float32`], [Índice de competencia real del proceso],
  [`estado_del_procedimiento`], [`string`], [Estado del proceso, relevante cuando no hay contrato],
  [`tiene_contrato`], [`bool`], [Indica si el proceso tiene un contrato adjudicado asociado],
  [`documento_proveedor`], [`string`], [NIT del proveedor ganador, vacío si tiene_contrato es falso],
  [`desviacion_precio`], [`float32`], [Desviación de precio respecto a categoría, si aplica],
  [`concentracion_proveedor`], [`float32`], [Concentración del proveedor con entidad, si aplica],
  [`puntuacion_riesgo`], [`float32`], [Puntuación combinada de riesgo],
  [`flag_riesgo_alto`], [`bool`], [Indicador si la combinación supera umbral de riesgo],
)

= Salida Final: Reporte de Resultados

El Master consolida los registros de riesgo y genera un reporte ordenado.

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  [`job_id`], [`string`], [Identificador del trabajo de análisis],
  [`execution_time_ms`], [`uint64`], [Tiempo total de cómputo en milisegundos],
  [`total_procesos_analizados`], [`uint64`], [Cantidad de procesos procesados],
  [`total_procesos_alto_riesgo`], [`uint64`], [Procesos que superan umbral de riesgo],
  [`registros_riesgo`], [`list<RiskRecord>`], [Lista de procesos de riesgo ordenados por puntuación],
)

Estructura RiskRecord:

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Campo*], [*Tipo*], [*Descripción*]),
  [`id_del_proceso`], [`string`], [Identificador del proceso],
  [`nit_entidad`], [`string`], [NIT de la entidad],
  [`nombre_entidad`], [`string`], [Nombre de la entidad],
  [`tiene_contrato`], [`bool`], [Si el proceso tiene contrato adjudicado asociado],
  [`documento_proveedor`], [`string`], [NIT del proveedor, vacío si no tiene contrato],
  [`nombre_proveedor`], [`string`], [Nombre del proveedor],
  [`valor_contrato`], [`uint64`], [Valor del contrato],
  [`indice_competencia`], [`float32`], [Índice de competencia observado],
  [`desviacion_precio`], [`float32`], [Desviación de precio],
  [`concentracion`], [`float32`], [Concentración del proveedor con entidad],
  [`puntuacion_riesgo`], [`float32`], [Puntuación final de riesgo combinada],
  [`flag_riesgo_alto`], [`bool`], [Indicador si la combinación supera umbral de riesgo],
)

= Especificación de contratos para gRPC / Protocol Buffers

Los contratos conceptuales anteriores se mapean a la siguiente definición formal en Protocol Buffers v3.

```protobuf
syntax = "proto3";

package secopanalysis;

option go_package = "./proto";

message ProcessDataChunk {
  string chunk_id = 1;
  string id_del_proceso = 2;
  string nit_entidad = 3;
  uint32 proveedores_invitados = 4;
  uint32 proveedores_unicos_con_respuestas = 5;
  string modalidad_de_contratacion = 6;
  string estado_del_procedimiento = 7;
  string nombre_entidad = 8;
}

message ContractDataChunk {
  string chunk_id = 1;
  string proceso_de_compra = 2;
  string nit_entidad = 3;
  string documento_proveedor = 4;
  string proveedor_adjudicado = 5;
  uint64 valor_del_contrato = 6;
  string codigo_de_categoria_principal = 7;
  string fecha_de_firma = 8;
  string nombre_entidad = 9;
}

message CompetitionMetrics {
  string id_del_proceso = 1;
  string nit_entidad = 2;
  uint32 proveedores_invitados = 3;
  uint32 proveedores_responden = 4;
  float indice_competencia = 5;
  string modalidad_de_contratacion = 6;
  string estado_del_procedimiento = 7;
}

// Reduce B1: desviación de precio, agrupado por categoría UNSPSC
message ContractPriceMetrics {
  string proceso_de_compra = 1;
  string nit_entidad = 2;
  string documento_proveedor = 3;
  uint64 valor_del_contrato = 4;
  string categoria_unspsc = 5;
  uint64 mediana_categoria = 6;
  float desviacion_precio = 7;
}

// Reduce B2: concentración, agrupado por documento_proveedor + nit_entidad
message ProviderConcentration {
  string documento_proveedor = 1;
  string nit_entidad = 2;
  uint32 contratos_con_entidad = 3;
  uint32 contratos_totales_proveedor = 4;
  float concentracion_proveedor = 5;
}

message RiskRecord {
  string id_del_proceso = 1;
  string nit_entidad = 2;
  string nombre_entidad = 3;
  bool tiene_contrato = 4;
  string documento_proveedor = 5;
  string nombre_proveedor = 6;
  uint64 valor_contrato = 7;
  float indice_competencia = 8;
  float desviacion_precio = 9;
  float concentracion = 10;
  float puntuacion_riesgo = 11;
  bool flag_riesgo_alto = 12;
}

message RiskAnalysisResult {
  string job_id = 1;
  uint64 execution_time_ms = 2;
  uint64 total_procesos_analizados = 3;
  uint64 total_procesos_alto_riesgo = 4;
  repeated RiskRecord registros_riesgo = 5;
}
```

== Aclaraciones de campos

La directiva repeated de Protocol Buffers serializa listas dinámicas de forma binaria eficiente, sin delimitadores de texto. Los enteros para valores monetarios y conteos se mapearon a uint64, mientras que los índices y proporciones se mapearon a float32 para balance entre precisión y tamaño. El job_id actúa como identificador global para indexar y almacenar los resultados de forma única.