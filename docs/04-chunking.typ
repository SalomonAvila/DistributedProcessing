#set document(
  title: "04 - Definición Matemática del Chunker",
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
  #text(size: 20pt, weight: "bold")[Definición matemática del Chunker y cálculo de particiones]

  #text(size: 13pt)[Algoritmo de particionamiento del dataset para procesamiento distribuido]

  #v(0.3em)
  #text(size: 10pt, fill: gray)[DistributedProcessing, Documento 04-definicion-chunker]
]

#v(1.5em)

La arquitectura planteada del sistema permite el procesamiento paralelo del dataset y su posterior integración en el nodo maestro, reduciendo los tiempos de procesamiento e incrementando la eficiencia.

Considerando que los tres nodos trabajadores cuentan con las mismas especificaciones en capacidad de procesamiento, memoria y almacenamiento, se implementará entonces un chunking de tamaño fijo donde cada chunk será del mismo tamaño. Cada uno de estos chunks será distribuido y procesado en un nodo trabajador para su posterior unificación.

= Fórmula y algoritmo de partición

Para el cálculo de cada chunk se tiene la siguiente fórmula, donde:

#table(
  columns: (auto, 1fr),
  stroke: 0.5pt + gray,
  inset: 8pt,
  align: left,
  table.header([*Variable*], [*Descripción*]),
  [`L`], [Tamaño total del archivo en bytes.],
  [`N`], [Número de chunks.],
  [`t_i`], [Límite del tamaño objetivo para el chunk $i$.],
)

El límite objetivo para cada chunk se calcula mediante:

#align(center)[
  $ t_i = frac(i L, N) $
]

Se plantea el siguiente algoritmo para la creación de chunks:

+ Calcular el límite para el chunk actual.
+ Realizar `seek()` hasta el límite calculado.
+ Si se llega a un delimitador, se inicia el siguiente chunk. De lo contrario, se avanza hasta el próximo delimitador.
+ La posición anterior se establece como el inicio del siguiente chunk.
+ Repetir el procedimiento hasta obtener $N - 1$ chunks.
+ El último chunk se obtiene utilizando la última posición calculada hasta el final del archivo.

= Prueba unitaria

Para comprobar el funcionamiento del algoritmo se utilizará una prueba unitaria que obtendrá los diferentes chunks y los concatenará, comprobando entonces que el archivo concatenado sea igual al archivo original.

La prueba realizará igualmente verificaciones sobre el tamaño del archivo y el número de líneas, con el objetivo de garantizar que el proceso de particionamiento no haya alterado ni perdido información del dataset original.

