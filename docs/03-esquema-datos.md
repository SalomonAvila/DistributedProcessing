# 03 - Esquema de Datos MapReduce

Este documento define los contratos y estructuras de datos para las diferentes fases del ciclo de vida de procesamiento distribuido MapReduce dentro del sistema de búsqueda de secuencias genómicas (FASTA). Se detallan los formatos del chunk de entrada, la salida intermedia de Map, la entrada de Reduce y el esquema de resultado final consolidado.

---

## 1. Fase de Entrada: Estructura del Chunk (`InputChunk`)

Cada Worker recibe un fragmento del genoma (chunk) generado por el Master o el servicio de particionado. Para evitar perder patrones en las fronteras de división, cada bloque incluye un margen de solapamiento (_overlap_ de $L - 1$ bytes, donde $L$ es la longitud del patrón buscado).

### 1.1 Definición de Campos

| Campo            | Tipo     | Descripción                                                          |
| :--------------- | :------- | :------------------------------------------------------------------- |
| `chunk_id`       | `string` | Identificador único del bloque (ej: `chr1_chunk_004`).               |
| `source_file`    | `string` | Nombre o identificador del archivo FASTA de origen.                  |
| `target_pattern` | `string` | Secuencia de ADN/patrón a buscar en este trabajo (ej: `ATGCGATC`).   |
| `start_offset`   | `uint64` | Posición global absoluta donde inicia el bloque propio en el genoma. |
| `end_offset`     | `uint64` | Posición global absoluta donde termina el bloque propio.             |
| `overlap_size`   | `uint32` | Número de bytes adicionales leídos del siguiente chunk ($L - 1$).    |
| `sequence_data`  | `string` | Cadena de nucleótidos (bloque propio + margen de overlap).           |

### 1.2 Aclaraciones de campos

- **`chunk_id` (`string`):** Es indispensable para la reasignación de tareas en caso de que un Worker falle.

- **`source_file` (`string`):** Permite que el sistema procese genomas divididos en múltiples archivos o ejecute trabajos sobre diferentes secuencias concurrentemente.

- **`start_offset` (`uint64`):** Indica la posición absoluta (en pares de bases) donde empieza el bloque dentro del genoma completo. Actúa como límite inferior para que el Worker descarte matches que inicien antes de este punto.

- **`end_offset` (`uint64`):** Representa el límite superior del bloque propio asignado al Worker. Sirve para evitar el conteo duplicado: aunque el Worker analice datos más allá de este punto debido al solapamiento, solo emitirá aquellos patrones cuyo inicio esté estrictamente en el intervalo $[start\_offset, end\_offset]$.

- **`overlap_size` (`uint32`):** Cuantifica exactamente cuántos bytes/nucleótidos extras contiene el bloque al final ($L - 1$). Esto permite al Worker validar la integridad de la ventana de lectura en los límites del chunk.

- **`sequence_data` (`string`):** Es la carga útil de procesamiento (_payload_).

---

## 2. Fase Intermedia: Pares Clave-Valor (`MapOutput`)

La función `Map` analiza el `sequence_data` y genera tuplas compuestas por una clave (patrón) y un objeto estructurado (`MatchInfo`) con el detalle de la posición localizada.

### 2.1 Estructura del Objeto `MatchInfo` (Value)

| Campo           | Tipo     | Descripción                                                      |
| :-------------- | :------- | :--------------------------------------------------------------- |
| `source_file`   | `string` | Archivo o secuencia de referencia donde se encontró el match.    |
| `global_offset` | `uint64` | Posición absoluta del primer nucleótido del patrón en el genoma. |
| `line`          | `uint32` | Número de línea en el archivo de origen.                         |
| `column`        | `uint32` | Columna o posición relativa local.                               |

### 2.2 Representación del Par Intermedio `(Key, Value)`

- **Key:** `pattern` (`string`) — Secuencia buscada o identificador del motivo.
- **Value:** `match` (`MatchInfo`) — Metadatos de la ocurrencia.

Formato del par:

```text
Key:   "ATGCGATC"
Value: MatchInfo {
    source_file:   "homo_sapiens_chr1.fasta",
    global_offset: 1450234,
    line:          18128,
    column:        34
}
```

### 2.3 Aclaraciones de campos

- **`Key` (`pattern` - `string`):** Establecer la clave como string permite que el sistema admita búsquedas simultáneas de múltiples motivos, por ejemplo, durante la fase de _Shuffle_, el cluster agrupará automáticamente todas las ocurrencias bajo su patrón correspondiente.

- **`Value` (`MatchInfo`):** Se estructura como un objeto compuesto en lugar de un número simple para transportar el contexto biológico completo de la aparición.
  - **`source_file` (`string`):** Permite conocer con precisión en qué cromosoma o archivo se produjo el hallazgo cuando se analizan genomas fragmentados.

  - **`global_offset` (`uint64`):** Coordenada absoluta global del inicio del patrón en el genoma. Es el dato principal para el ordenamiento posterior y el mapeo genómico.

  - **`line` (`uint32`):** Número de línea dentro del archivo FASTA original. Facilita la verificación manual e inspección rápida abriendo el archivo fuente en editores o herramientas de bioinformática.

  - **`column` (`uint32`):** Posición horizontal dentro de la línea del archivo de origen, completando las coordenadas exactas de localización.

---

## 3. Fase de Reducción: Entrada de Reduce (`ReduceInput`)

El proceso de _Shuffle / Partitioning_ agrupa todos los valores intermedios emitidos por los Workers asociados a una misma clave antes de suministrarlos a la función reductora.

### 3.1 Definición

- **Key:** `pattern` (`string`).
- **Values:** `list<MatchInfo>` — Colección iterable de todas las ocurrencias emitidas por los diferentes Workers para ese patrón.

### 3.2 Aclaraciones de campos

- **`Key` (`pattern` - `string`):** Identifica el grupo de datos que entra a reducirse. Garantiza que cada función Reduce trabaje de forma aislada sobre un único motivo o subsecuencia.

- **`Values` (`list<MatchInfo>`):** Colección de todos los objetos de coincidencia generados por la fase Map a lo largo del cluster. Al recibir la lista completa en memoria/stream, el Reducer tiene los elementos necesarios tanto para contar el total de frecuencias como para ejecutar un algoritmo de ordenamiento sobre las posiciones encontradas.

---

## 4. Fase de Salida: Resultado Final (`FinalResult`)

El resultado que produce la función `Reduce` consolida la frecuencia total y el inventario ordenado de ubicaciones encontradas.

### 4.1 Definición de Campos

| Campo               | Tipo              | Descripción                                                     |
| :------------------ | :---------------- | :-------------------------------------------------------------- |
| `job_id`            | `string`          | Identificador del trabajo de procesamiento.                     |
| `pattern`           | `string`          | Patrón consultado.                                              |
| `total_matches`     | `uint64`          | Número total acumulado de coincidencias encontradas.            |
| `matches`           | `list<MatchInfo>` | Lista de todas las coincidencias ordenadas por `global_offset`. |
| `execution_time_ms` | `uint64`          | Tiempo total de cómputo transcurrido (para métricas).           |

### 4.2 Aclaraciones de campos

- **`job_id` (`string`):** Identificador global del trabajo de búsqueda ejecutado. Permite indexar, almacenar y consultar los resultados de forma unívoca en la base de datos de resultados.

- **`pattern` (`string`):** Confirma la secuencia de ADN asociada a la respuesta devuelta al cliente o usuario solicitante.

- **`total_matches` (`uint64`):** Conteo acumulado de ocurrencias. Proveerlo como un campo explícito ahorra a las aplicaciones cliente tener que recorrer o medir la lista de resultados únicamente para saber cuántas veces apareció el patrón.

- **`matches` (`list<MatchInfo>`):** Arreglo final consolidado con todos los detalles de ubicación. Esta lista se entrega ordenada cronológicamente por `global_offset`, lista para graficar o generar reportes bioinformáticos.

- **`execution_time_ms` (`uint64`):** Tiempo que tomó la ejecución del procesamiento. Es vital para las métricas de rendimiento del cluster, benchmarks y evaluación de throughput del sistema distribuido.

---

## 5. Especificación de Contratos para gRPC / Protocol Buffers

Dado que la arquitectura se implementa sobre Go y gRPC, los contratos conceptuales anteriores se mapean a la siguiente definición formal en Protocol Buffers v3:

```protobuf
syntax = "proto3";

package mapreduce;

option go_package = "./proto";

// Representa el bloque asignado al Worker
message InputChunk {
  string chunk_id = 1;
  string source_file = 2;
  string target_pattern = 3;
  uint64 start_offset = 4;
  uint64 end_offset = 5;
  uint32 overlap_size = 6;
  string sequence_data = 7;
}

// Representa una coincidencia puntual
message MatchInfo {
  string source_file = 1;
  uint64 global_offset = 2;
  uint32 line = 3;
  uint32 column = 4;
}

// Mensaje emitido por el Worker tras procesar el chunk
message MapResponse {
  string chunk_id = 1;
  string pattern = 2;
  repeated MatchInfo matches = 3;
}

// Estructura consolidada entregada por el Reduce y persistida
message FinalResult {
  string job_id = 1;
  string pattern = 2;
  uint64 total_matches = 3;
  repeated MatchInfo matches = 4;
  uint64 execution_time_ms = 5;
}
```

### 5.1 Aclaraciones de campos

- **`repeated MatchInfo matches`:** En Protocol Buffers, la directiva `repeated` serializa una lista dinámica de forma binaria eficiente, eliminando delimitadores de texto (como corchetes o comas de JSON) y reduciendo el tráfico de red en el cluster.

- **Compatibilidad de tipos escalares:** Se mapearon los enteros grandes a `uint64` (para offsets y métricas) y los índices locales a `uint32` (para filas y columnas), optimizando el empaquetado binario mediante codificación varint de Protocol Buffers.
