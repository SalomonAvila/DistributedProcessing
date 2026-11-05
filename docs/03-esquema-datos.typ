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

= Conjuntos de datos de origen y selección de columnas <columnas>

El sistema usa dos conjuntos de datos de SECOP II publicados por Colombia Compra Eficiente en datos.gov.co. El número de columnas se verificó contra los metadatos del portal el 5 de noviembre de 2026:

#table(
  columns: (1fr, auto, auto, auto),
  stroke: 0.5pt + gray,
  inset: 7pt,
  align: left,
  table.header([*Conjunto de datos*], [*Identificador*], [*Columnas*], [*Usadas por el motor*]),
  [SECOP II – Procesos de Contratación], [`p6dx-8zbt`], [59], [7],
  [SECOP II – Contratos Electrónicos], [`jbjy-vk9h`], [95], [8],
)

El esquema publicado cambia con el tiempo. Por ejemplo, Contratos Electrónicos pasó de 85 a 95 columnas al incorporar diez campos ambientales y de reversión. El Chunker localiza cada columna por su nombre de encabezado y no por su posición, de modo que agregar o reordenar columnas no afecta al motor. Solo lo afecta que se renombre o elimine una de las columnas usadas.

== Por qué el motor usa solo una fracción de las columnas

Una columna entra al motor únicamente si un Job o el join la necesita para calcular o identificar un indicador. Hay tres razones:

+ *Costo de red:* el Master lee el CSV y envía cada chunk a los Workers por gRPC. Luego, el Shuffle redistribuye los pares clave–valor entre Workers. Todo byte que entra al motor viaja al menos dos veces por la red, y medir ese tráfico es uno de los objetivos del proyecto. En la muestra real del repositorio, las columnas usadas representan cerca del 11~% de los bytes de Procesos y el 9~% de los de Contratos.
+ *Memoria:* los registros intermedios se agrupan en memoria durante el Shuffle y el Reduce. Proyectar solo los campos necesarios reduce el tamaño de cada registro en el mismo orden.
+ *Alcance y privacidad:* la mayoría de las columnas no interviene en ninguno de los tres indicadores, y varias contienen datos personales que no deben circular por el sistema.

La proyección ocurre en el Chunker. Al convertir cada fila en un mensaje `ProcessDataChunk` o `ContractDataChunk`, se descarta el resto de la fila.

== Columnas usadas por el motor

#table(
  columns: (auto, auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 7pt,
  align: left,
  table.header([*Columna en el portal*], [*Conjunto*], [*Uso*]),
  [ID del Proceso], [Procesos], [Clave del Job A y clave del join (Etapa 2).],
  [Nit Entidad], [Procesos], [Identifica la entidad en el resultado del Job A.],
  [Entidad], [Procesos], [Nombre de la entidad para el reporte.],
  [Proveedores Invitados], [Procesos], [Denominador del índice de competencia (Job A).],
  [Proveedores Unicos con Respuestas], [Procesos], [Numerador del índice de competencia (Job A).],
  [Modalidad de Contratacion], [Procesos], [Contexto del proceso: la competencia esperada depende de la modalidad.],
  [Estado del Procedimiento], [Procesos], [Distingue procesos adjudicados, desiertos o en curso.],
  [Proceso de Compra], [Contratos], [Clave del join con Procesos.],
  [Nit Entidad], [Contratos], [Parte de la clave proveedor–entidad del Job B2.],
  [Nombre Entidad], [Contratos], [Nombre de la entidad para el reporte.],
  [Documento Proveedor], [Contratos], [Parte de la clave proveedor–entidad del Job B2.],
  [Proveedor Adjudicado], [Contratos], [Nombre del proveedor para el reporte.],
  [Valor del Contrato], [Contratos], [Valor comparado contra la mediana de la categoría (Job B1).],
  [Codigo de Categoria Principal], [Contratos], [Categoría UNSPSC, clave de agrupación del Job B1.],
  [Fecha de Firma], [Contratos], [Ubica el contrato en el tiempo para filtrar por periodo.],
)

== Columnas excluidas y motivo

El resto de las columnas se clasifica en cuatro grupos:

- *C – Candidata para la capa de consulta:* no interviene en los indicadores, pero puede ser necesaria para responder preguntas en lenguaje natural. Incluye ubicación y sector de la entidad, fechas de publicación y adjudicación, textos libres (objeto, descripción y justificación de la modalidad) y la URL del proceso. Estas columnas no pasan por el motor. Si se cargan, irán directamente de los datos procesados a PostgreSQL. Qué columnas se incluyen es una decisión abierta (ver `04-rag`, decisiones abiertas).
- *P – Datos personales:* nombres, documentos y domicilio de representantes legales, supervisores, ordenadores del gasto y de pago, el nombre del usuario que adjudicó y los datos bancarios del contrato. Se excluyen del motor, de PostgreSQL, de los embeddings y del contexto del LLM. No aportan a los indicadores y su exposición en una interfaz abierta no está justificada.
- *I – Identificador interno o redundante:* códigos internos de la plataforma, referencias asignadas por la entidad, identificadores de estado o tipo de documento y campos que repiten información de otra columna, como Localización frente a Departamento y Ciudad.
- *O – Fuera del alcance de los indicadores:* fechas de cada fase del proceso, conteos de respuestas distintos del de proveedores únicos, ejecución financiera (valores facturados, pagados y amortizados, saldos), fuentes de los recursos, campos de posconflicto, documentos tipo y campos ambientales, de posconsumo y de reversión. Podrían alimentar indicadores futuros, pero no los tres definidos en este proyecto.

== Anexo: clasificación de todas las columnas

Clave: *M* usada por el motor; *C* candidata para la capa de consulta; *P* dato personal (excluida); *I* identificador interno o redundante; *O* fuera del alcance de los indicadores.

=== SECOP II – Procesos de Contratación (59 columnas)

Resumen: M = 7, C = 19, P = 1, I = 7, O = 25.

#text(size: 9pt)[
#table(
  columns: (auto, 1fr, auto),
  stroke: 0.4pt + gray,
  inset: 4pt,
  align: (right, left, center),
  table.header([*\#*], [*Columna en el portal*], [*Uso*]),
  [1], [Entidad], [M],
  [2], [Nit Entidad], [M],
  [3], [Departamento Entidad], [C],
  [4], [Ciudad Entidad], [C],
  [5], [OrdenEntidad], [C],
  [6], [Entidad Centralizada], [O],
  [7], [ID del Proceso], [M],
  [8], [Referencia del Proceso], [I],
  [9], [PCI], [I],
  [10], [ID del Portafolio], [I],
  [11], [Nombre del Procedimiento], [C],
  [12], [Descripción del Procedimiento], [C],
  [13], [Fase], [O],
  [14], [Fecha de Publicacion del Proceso], [C],
  [15], [Fecha de Ultima Publicación], [O],
  [16], [Fecha de Publicacion (Fase Planeacion Precalificacion)], [O],
  [17], [Fecha de Publicacion (Fase Seleccion Precalificacion)], [O],
  [18], [Fecha de Publicacion (Manifestacion de Interes)], [O],
  [19], [Fecha de Publicacion (Fase Borrador)], [O],
  [20], [Fecha de Publicacion (Fase Seleccion)], [O],
  [21], [Precio Base], [C],
  [22], [Modalidad de Contratacion], [M],
  [23], [Justificación Modalidad de Contratación], [C],
  [24], [Duracion], [O],
  [25], [Unidad de Duracion], [O],
  [26], [Fecha de Recepcion de Respuestas], [O],
  [27], [Fecha de Apertura de Respuesta], [O],
  [28], [Fecha de Apertura Efectiva], [O],
  [29], [Ciudad de la Unidad de Contratación], [O],
  [30], [Nombre de la Unidad de Contratación], [O],
  [31], [Proveedores Invitados], [M],
  [32], [Proveedores con Invitacion Directa], [O],
  [33], [Visualizaciones del Procedimiento], [O],
  [34], [Proveedores que Manifestaron Interes], [C],
  [35], [Respuestas al Procedimiento], [O],
  [36], [Respuestas Externas], [O],
  [37], [Conteo de Respuestas a Ofertas], [O],
  [38], [Proveedores Unicos con Respuestas], [M],
  [39], [Numero de Lotes], [O],
  [40], [Estado del Procedimiento], [M],
  [41], [ID Estado del Procedimiento], [I],
  [42], [Adjudicado], [C],
  [43], [ID Adjudicacion], [I],
  [44], [CodigoProveedor], [I],
  [45], [Departamento Proveedor], [C],
  [46], [Ciudad Proveedor], [C],
  [47], [Fecha Adjudicacion], [C],
  [48], [Valor Total Adjudicacion], [C],
  [49], [Nombre del Adjudicador], [P],
  [50], [Nombre del Proveedor Adjudicado], [C],
  [51], [NIT del Proveedor Adjudicado], [C],
  [52], [Codigo Principal de Categoria], [C],
  [53], [Estado de Apertura del Proceso], [O],
  [54], [Tipo de Contrato], [C],
  [55], [Subtipo de Contrato], [O],
  [56], [Categorias Adicionales], [O],
  [57], [URLProceso], [C],
  [58], [Codigo Entidad], [I],
  [59], [Estado Resumen], [O],
)
]

=== SECOP II – Contratos Electrónicos (95 columnas)

Resumen: M = 8, C = 17, P = 18, I = 6, O = 46.

#text(size: 9pt)[
#table(
  columns: (auto, 1fr, auto),
  stroke: 0.4pt + gray,
  inset: 4pt,
  align: (right, left, center),
  table.header([*\#*], [*Columna en el portal*], [*Uso*]),
  [1], [Nombre Entidad], [M],
  [2], [Nit Entidad], [M],
  [3], [Departamento], [C],
  [4], [Ciudad], [C],
  [5], [Localización], [I],
  [6], [Orden], [C],
  [7], [Sector], [C],
  [8], [Rama], [C],
  [9], [Entidad Centralizada], [O],
  [10], [Proceso de Compra], [M],
  [11], [ID Contrato], [C],
  [12], [Referencia del Contrato], [I],
  [13], [Estado Contrato], [C],
  [14], [Codigo de Categoria Principal], [M],
  [15], [Descripcion del Proceso], [C],
  [16], [Tipo de Contrato], [C],
  [17], [Modalidad de Contratacion], [C],
  [18], [Justificacion Modalidad de Contratacion], [C],
  [19], [Fecha de Firma], [M],
  [20], [Fecha de Inicio del Contrato], [C],
  [21], [Fecha de Fin del Contrato], [C],
  [22], [Condiciones de Entrega], [O],
  [23], [TipoDocProveedor], [I],
  [24], [Documento Proveedor], [M],
  [25], [Proveedor Adjudicado], [M],
  [26], [Es Grupo], [O],
  [27], [Es Pyme], [C],
  [28], [Habilita Pago Adelantado], [O],
  [29], [Liquidación], [O],
  [30], [Obligación Ambiental], [O],
  [31], [Obligaciones Postconsumo], [O],
  [32], [Reversion], [O],
  [33], [Origen de los Recursos], [O],
  [34], [Destino Gasto], [O],
  [35], [Valor del Contrato], [M],
  [36], [Valor de pago adelantado], [O],
  [37], [Valor Facturado], [O],
  [38], [Valor Pendiente de Pago], [O],
  [39], [Valor Pagado], [O],
  [40], [Valor Amortizado], [O],
  [41], [Valor Pendiente de Amortizacion], [O],
  [42], [Valor Pendiente de Ejecucion], [O],
  [43], [Saldo CDP], [O],
  [44], [Saldo Vigencia], [O],
  [45], [EsPostConflicto], [O],
  [46], [Dias adicionados], [C],
  [47], [Puntos del Acuerdo], [O],
  [48], [Pilares del Acuerdo], [O],
  [49], [URLProceso], [C],
  [50], [Nombre Representante Legal], [P],
  [51], [Nacionalidad Representante Legal], [P],
  [52], [Domicilio Representante Legal], [P],
  [53], [Tipo de Identificación Representante Legal], [P],
  [54], [Identificación Representante Legal], [P],
  [55], [Género Representante Legal], [P],
  [56], [Presupuesto General de la Nacion – PGN], [O],
  [57], [Sistema General de Participaciones], [O],
  [58], [Sistema General de Regalías], [O],
  [59], [Recursos Propios (Alcaldías y Gobernaciones)], [O],
  [60], [Recursos de Credito], [O],
  [61], [Otros Recursos (Especie, Privados, Cooperación, Propios Entidades Autónomas)], [O],
  [62], [Ultima Actualizacion], [I],
  [63], [Codigo Entidad], [I],
  [64], [Codigo Proveedor], [I],
  [65], [Fecha Inicio Liquidacion], [O],
  [66], [Fecha Fin Liquidacion], [O],
  [67], [Objeto del Contrato], [C],
  [68], [Duración del contrato], [O],
  [69], [Nombre del banco], [P],
  [70], [Tipo de cuenta], [P],
  [71], [Número de cuenta], [P],
  [72], [El contrato puede ser prorrogado], [O],
  [73], [Fecha de notificación de prorrogación], [O],
  [74], [Nombre ordenador del gasto], [P],
  [75], [Tipo de documento Ordenador del gasto], [P],
  [76], [Número de documento Ordenador del gasto], [P],
  [77], [Nombre supervisor], [P],
  [78], [Tipo de documento supervisor], [P],
  [79], [Número de documento supervisor], [P],
  [80], [Nombre Ordenador de Pago], [P],
  [81], [Tipo de documento Ordenador de Pago], [P],
  [82], [Número de documento Ordenador de Pago], [P],
  [83], [Documentos Tipo], [O],
  [84], [Descripcion Documentos Tipo], [O],
  [85], [Dirección de ejecución del contrato], [O],
  [86], [Criterios de sostenibilidad ambiental], [O],
  [87], [Requisitos ambientales en las especificaciones técnicas], [O],
  [88], [Criterios ambientales en la evaluación de las ofertas], [O],
  [89], [Obligaciones contractuales o poscontractuales a nivel ambiental], [O],
  [90], [Uso de etiquetado ecológico], [O],
  [91], [Obligaciones posconsumo], [O],
  [92], [Fecha inicio obligaciones posconsumo], [O],
  [93], [Fecha fin obligaciones posconsumo], [O],
  [94], [Fecha inicio reversión], [O],
  [95], [Fecha fin reversión], [O],
)
]

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