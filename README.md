# DistributedProcessing

Motor de procesamiento distribuido, construido desde cero en Go, para el análisis de riesgo en la contratación pública colombiana a partir de los datasets abiertos de SECOP II. El sistema implementa el paradigma MapReduce sobre una arquitectura Master Worker con un master y tres workers, orquestada mediante Kubernetes.

## Qué problema resuelve

SECOP II publica la contratación pública de Colombia en dos conjuntos de datos separados, sin que exista un indicador consolidado que los cruce.

Procesos de Contratación contiene 9,14 millones de filas con actualización diaria. Registra el embudo de competencia de cada proceso: proveedores invitados, proveedores que manifestaron interés, proveedores únicos con respuesta.

Contratos Electrónicos contiene aproximadamente 6 millones de filas. Registra el contrato ya adjudicado: proveedor ganador, entidad, valor, categoría UNSPSC y modalidad de contratación.

El sistema cruza ambas fuentes para producir un indicador conjunto de riesgo por proceso, proveedor y entidad, combinando tres señales. La primera es el índice de competencia real, definido como los proveedores que efectivamente respondieron frente a los invitados. La segunda es la desviación de precio respecto a la mediana de su categoría UNSPSC. La tercera es la concentración proveedor entidad, entendida como el porcentaje de contratos que un mismo proveedor gana con una misma entidad.

El resultado es un ranking de procesos de alto riesgo, exportable en CSV, replicable sobre cualquier período del histórico de SECOP II. El sistema no sustituye a organismos de control. Es una señal preliminar para investigación posterior, no una conclusión de irregularidad.

## Por qué es un problema de procesamiento distribuido

El volumen total, entre veinte y cuarenta gigabytes crudos entre ambos datasets, cabe cómodo en el clúster disponible. El reto real no es el volumen sino el join distribuido. El pipeline corre en dos etapas.

La Etapa 1 ejecuta tres jobs de reducción en paralelo, sin dependencia entre sí. El Job A agrupa por identificador de proceso y calcula el índice de competencia. El Job B1 agrupa por categoría UNSPSC y calcula la mediana de precio junto con la desviación por contrato. El Job B2 agrupa por la combinación de proveedor y entidad y calcula la concentración.

La Etapa 2 ejecuta un join distribuido de tipo reduce side join que combina los tres resultados intermedios de la Etapa 1, no los datos crudos, por identificador de proceso, etiquetando cada registro por su origen antes del shuffle. Los procesos sin contrato asociado, es decir no adjudicados o desiertos, se conservan mediante un left outer join en lugar de descartarse, ya que constituyen un hallazgo propio de interés.

## Hardware del clúster

El clúster está compuesto por cuatro máquinas físicas idénticas: un nodo master y tres nodos worker. Cada nodo cuenta con veinte núcleos, 64 GB de RAM y 100 GB de almacenamiento. La capacidad agregada del clúster es de 80 núcleos, 256 GB de RAM y 300 GB de almacenamiento.

## Decisiones de arquitectura

El sistema utiliza Go como lenguaje principal, aprovechando su concurrencia nativa mediante goroutines y sus binarios estáticos de tamaño reducido. La comunicación entre master y workers se realiza mediante gRPC sobre Protocol Buffers, cubriendo asignación de tareas, heartbeats y transmisión de chunks. Los binarios se empaquetan mediante Docker. La orquestación del ciclo de vida de los contenedores corre sobre Kubernetes.

La separación de responsabilidades es deliberada. Kubernetes resuelve la orquestación de procesos: el master se despliega mediante un Deployment, los workers mediante un StatefulSet con Service headless para obtener nombres DNS estables, se configura podAntiAffinity para que cada worker se programe en un nodo físico distinto, y se usan livenessProbe y readinessProbe para el reinicio automático de contenedores caídos. El motor propio resuelve la distribución de datos y cómputo: particionamiento en chunks, protocolo de asignación de tareas, shuffle, join distribuido y tolerancia a fallos a nivel de tarea mediante detección de heartbeat perdido y reasignación del chunk a otro worker.

Deliberadamente no se delega en primitivas nativas de Kubernetes la responsabilidad de repartir el trabajo entre workers, ya que esa lógica de particionamiento y scheduling es el objeto de aprendizaje e implementación del proyecto.

Como consecuencia de esta separación, existen dos mecanismos de recuperación ante fallos que operan en planos distintos y se miden por separado. La recuperación de la tarea ocurre cuando el master reasigna el chunk de un worker caído, con una meta de quince segundos o menos. La recuperación del proceso ocurre cuando Kubernetes reprograma el pod eliminado según sus probes de salud.

## Documentación completa

La documentación formal del proyecto vive en la carpeta docs en formato Typst y se compila automáticamente a PDF en la carpeta RenderedDocuments mediante GitHub Actions al hacer push a la rama principal.

El documento 00 propuesta contiene el objetivo general, los objetivos específicos, las fuentes de datos y la justificación del proyecto.

El documento 01 alcance contiene el problema que resuelve el sistema, los no objetivos, las restricciones de red, el hardware disponible, el volumen de datos esperado y las métricas de éxito, incluyendo el speedup mínimo, el throughput, los tiempos de recuperación y la verificación de corrección contra un script de referencia secuencial.

El documento 02 arquitectura contiene la justificación y los trade offs de cada decisión técnica, la matriz de alternativas evaluadas, y los diagramas de despliegue y del pipeline de dos etapas.

El documento 03 esquema de datos contiene los contratos de datos completos: la estructura de los chunks de entrada, las salidas intermedias de cada reduce, la mecánica del join distribuido, y la definición formal en Protocol Buffers.

Los diagramas en formato drawio, correspondientes a componentes, despliegue y secuencia, están en la carpeta docs y se exportan a PNG en el mismo pipeline.

## Estado actual del código

Actualmente solo está implementado el esqueleto mínimo de conectividad gRPC entre un master y un worker, con un procedimiento remoto de prueba llamado Ping. Esto valida que la tubería completa entre el archivo proto, la generación de código gRPC y los binarios de Go funciona de punta a punta. Todavía no se ha implementado ninguna lógica de MapReduce, incluyendo particionamiento, shuffle, heartbeats y reasignación de tareas, ni los jobs de análisis de riesgo.

La estructura del código fuente sigue la convención estándar de Go. El punto de entrada del master y del worker vive cada uno en su propia carpeta dentro de cmd. El archivo proto contiene por ahora únicamente el servicio de prueba Ping, y falta ampliarlo con los mensajes definidos en el documento de esquema de datos.

### Pendiente de implementar

El primer paso pendiente es ampliar el archivo proto con los mensajes reales del esquema de datos y regenerar el código correspondiente.

El segundo paso es construir el motor MapReduce genérico, cubriendo particionamiento en chunks, asignación de tareas, heartbeats, shuffle y reasignación ante fallo, validado primero con una carga trivial antes de incorporar la lógica de negocio.

El tercer paso es implementar la lógica de negocio correspondiente al Job A, al Job B1, al Job B2 y al join distribuido de la Etapa 2, cada uno validado contra un script de referencia secuencial sobre una muestra real de datos.

El cuarto paso es escribir los manifiestos de Kubernetes correspondientes al Deployment del master y al StatefulSet con Service headless de los workers, incluyendo la configuración de podAntiAffinity.

El quinto paso es instrumentar las métricas de speedup, throughput y tiempos de recuperación en los dos planos, como insumo directo para los experimentos formales del informe.

### Cuestiones pendientes de corregir

El Dockerfile del worker actualmente compila un binario a partir de un archivo que no corresponde al punto de entrada real del worker. Debe corregirse para que compile desde la carpeta correcta y produzca el binario esperado.

El Dockerfile del master utiliza una ruta de compilación que no corresponde a la estructura real del código fuente y debe corregirse de forma equivalente.

El algoritmo de particionamiento del chunker aún no resuelve si el número de particiones debe ser igual al número de workers, con un chunk grande por worker, o considerablemente mayor, con cada worker procesando varios chunks pequeños en cola. Esta segunda opción es la que exige el documento de alcance para permitir balanceo fino y reasignación rápida ante fallos, y todavía no está resuelta en el diseño del chunker.

El archivo de composición de Docker sirve únicamente para desarrollo local. Utiliza nombres de host fijos para cada worker y no representa la topología de despliegue real, que corre sobre Kubernetes. Tampoco tiene configurada verificación de salud de los contenedores, por lo que no sirve para validar la recuperación automática de procesos. Esa prueba solo es representativa en el clúster real.

El archivo README de la carpeta docs quedó desactualizado. Describe a Kubernetes como pendiente por implementar y describe el esquema de datos con los nombres correspondientes a una versión anterior del proyecto, orientada al alineamiento de secuencias genómicas. Debe alinearse con el estado actual de los documentos de arquitectura y esquema de datos.

## Cómo correr el esqueleto actual

Para correr el worker de forma local, dentro de la carpeta src se ejecuta el comando go run apuntando a la carpeta cmd worker. En otra terminal, dentro de la misma carpeta src, se ejecuta el comando go run apuntando a la carpeta cmd master.

Una vez corregidos los Dockerfiles, el sistema completo puede levantarse mediante el comando docker compose up con la opción de reconstrucción de imágenes.

## Métricas de éxito del proyecto

El sistema debe alcanzar un speedup mínimo de dos veces sobre los tres workers frente a la ejecución secuencial equivalente. Debe sostener un throughput agregado mínimo de cincuenta megabytes por segundo, medido por separado para la Etapa 1 y la Etapa 2. La recuperación de tarea debe tomar quince segundos o menos, mientras que la recuperación de proceso mediante Kubernetes se mide de forma independiente. La corrección del sistema debe verificarse por comparación exacta contra un script secuencial de referencia sobre una muestra de datos, incluyendo explícitamente el caso de procesos sin contrato asociado.

El detalle completo de cada métrica está en el documento de alcance dentro de la carpeta docs.