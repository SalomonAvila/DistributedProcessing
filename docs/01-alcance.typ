#set document(
  title: "01 - Alcance del Proyecto",
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
  #text(size: 20pt, weight: "bold")[Alcance del Proyecto]

  #text(size: 13pt)[Motor de Procesamiento Distribuido para Alineamiento de Secuencias Genómicas]

  #v(0.3em)
  #text(size: 10pt, fill: gray)[DistributedProcessing, Documento 01-alcance]
]

#v(1.5em)

= Problema que resuelve el sistema

El análisis de datos bioinformáticos, en particular la búsqueda y el alineamiento de patrones (secuencias de ADN) dentro de genomas completos, es una tarea computacionalmente intensiva. Los archivos FASTA que representan genomas reales pueden ocupar varios gigabytes, y buscar la ocurrencia de uno o varios patrones dentro de ellos mediante un único hilo de ejecución se vuelve impráctico: los tiempos de cómputo crecen linealmente con el tamaño del genoma y el proceso queda limitado por la memoria RAM disponible en una sola máquina.

El sistema resuelve este problema mediante un motor de procesamiento distribuido, construido desde cero y sin apoyarse en frameworks de terceros, que implementa el paradigma *MapReduce* sobre una arquitectura *Master-Worker*. El genoma de entrada se particiona en bloques (*chunks*) con solapamiento de fronteras, cada Worker procesa su bloque en paralelo durante la fase Map, y el Master consolida los resultados agrupados por patrón durante la fase Reduce, produciendo un listado ordenado y deduplicado de coincidencias junto con métricas de rendimiento.

En síntesis, el sistema busca reducir el tiempo de búsqueda de patrones genómicos mediante paralelización real en múltiples nodos físicos, ofrecer tolerancia a fallos de nodos Worker durante la ejecución de un trabajo, y servir como banco de pruebas controlado para medir el comportamiento de un sistema distribuido concurrente frente al procesamiento secuencial equivalente.

= No-objetivos

El sistema explícitamente no busca ser un alineador biológico de referencia: no reemplaza ni compite en precisión biológica con herramientas de producción especializadas del ámbito bioinformático; el alineamiento implementado es una búsqueda de patrones exactos o aproximados por bloques con fines académicos y de medición de rendimiento, no de uso clínico ni de investigación genómica formal.

Tampoco implementa orquestación dinámica de clúster. La topología es fija, con un Master y tres Workers; no hay autoescalado, ni incorporación o remoción de nodos en caliente, ni integración con un orquestador de contenedores en esta fase del proyecto.

El sistema no provee alta disponibilidad del Master, que constituye un único punto de fallo; no se implementa replicación ni failover automático del nodo maestro. Tampoco incluye autenticación, autorización ni cifrado en tránsito: la comunicación entre nodos opera sobre una red confiable y cerrada, sin control de acceso.

El sistema no es una plataforma de procesamiento en tiempo real ni de streaming. Los trabajos se ejecutan en modo batch: un genoma y uno o varios patrones se cargan, se procesan y se entrega un resultado consolidado al finalizar. Tampoco incluye interfaz gráfica de usuario; la interacción se realiza vía API o línea de comandos. Finalmente, el sistema no garantiza tolerancia ante partición de red: se asume una red físicamente estable durante la ejecución de un trabajo, y los escenarios de partición parcial quedan fuera del alcance de las pruebas de tolerancia a fallos.

= Restricciones de red

Las cuatro máquinas del clúster están conectadas físicamente mediante Ethernet, sobre una red local cerrada dedicada al clúster. Al tratarse de una LAN cableada entre los cuatro hosts en el mismo segmento, se estima una latencia punto a punto menor o igual a *1 ms* para el tráfico gRPC de control. Este valor es un estimado inicial de diseño y debe validarse empíricamente una vez desplegado el clúster.

Se asume un enlace Gigabit Ethernet por nodo, sin garantía de ancho de banda dedicado si la red llegara a compartirse con otro tráfico. El protocolo de comunicación es gRPC sobre HTTP/2, lo que implica sensibilidad tanto a la latencia de establecimiento de conexión como a la pérdida de paquetes en el plano de control, donde viajan los heartbeats y la reasignación de chunks. Se asume además que la red no sufre particiones durante la ejecución de un trabajo; el mecanismo de tolerancia a fallos cubre caídas de proceso o de nodo Worker, no fallos de red prolongados o intermitentes.

= Hardware disponible por nodo

El clúster está compuesto por cuatro máquinas físicas con hardware idéntico: un nodo Master y tres nodos Worker.

#table(
  columns: (auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: (left, left),
  table.header([*Especificación*], [*Valor por nodo*]),
  [Rol], [1 Master, 3 Workers, hardware idéntico entre todos los nodos],
  [CPU], [20 núcleos],
  [RAM], [64 GB],
  [Red], [Ethernet, interfaz cableada dedicada al clúster],
  [Contenedorización], [Docker, cada nodo ejecuta su rol en contenedor],
)

Con los cuatro nodos idénticos, la capacidad agregada del clúster es de 80 núcleos y 256 GB de RAM en total. El Master, además de coordinar la asignación de chunks y consolidar los resultados en la fase Reduce, comparte el mismo perfil de hardware que los Workers, por lo que no constituye un cuello de botella de cómputo: su rol es principalmente de coordinación y agregación, no de procesamiento intensivo de datos.

= Volumen de datos esperado y tamaño de dataset soportado

Los datos de entrada corresponden a archivos FASTA en texto plano con secuencias de nucleótidos, potencialmente divididos en múltiples archivos por cromosoma o secuencia mediante el campo `source_file`. El tamaño de dataset con el que se trabaja actualmente es de 1 GB, muy por debajo de la capacidad de RAM de cada Worker, lo que permite que un chunk completo, junto con su margen de overlap y las estructuras intermedias de Map, resida cómodamente en memoria sin acercarse al límite físico del nodo.

El tamaño de cada chunk individual es configurable por el Master; se recomienda mantenerlo en el orden de decenas o cientos de MB, muy por debajo del límite de RAM por nodo, para permitir balanceo de carga fino, reasignación rápida ante fallos y transmisión por streaming de gRPC en lugar de mensajes únicos de gran tamaño, ya que gRPC impone un límite de tamaño de mensaje que hace impráctico enviar un chunk completo en una sola llamada si este creciera a varios gigabytes. Este límite de 1 GB corresponde al dataset actual del proyecto y deberá revisarse si el volumen de datos crece en fases posteriores.

= Métricas de éxito

== Throughput mínimo esperado

El sistema debe sostener un speedup mínimo de 2x frente al procesamiento secuencial equivalente al ejecutar sobre los tres Workers, como umbral mínimo para justificar la arquitectura distribuida frente a un enfoque secuencial. Se define como meta un throughput agregado mínimo de 50 MB/s de datos genómicos procesados en la fase Map por el clúster completo, medido como el tamaño total del dataset dividido entre el `execution_time_ms` reportado en el `FinalResult`. Estos números son objetivos iniciales a validar empíricamente; el proyecto contempla medir y reportar el speedup y el throughput reales obtenidos como parte de sus objetivos específicos, no solo cumplir un umbral fijo.

== Tiempo máximo de recuperación ante fallo

Ante la caída de un nodo Worker durante la ejecución de un trabajo, el Master debe detectar el fallo, mediante la ausencia del heartbeat o el timeout de la tarea, en un máximo de 10 segundos. Tras la detección, el Master debe reasignar el chunk afectado a otro Worker disponible en un máximo adicional de 5 segundos. En conjunto, el tiempo máximo de recuperación ante fallo de un nodo Worker, desde la caída hasta que el chunk vuelve a estar en procesamiento en otro nodo, debe ser menor o igual a 15 segundos. La caída del nodo Master se considera fuera de alcance de recuperación automática, según lo indicado en la sección de no-objetivos: un fallo del Master implica la pérdida del trabajo en curso.
