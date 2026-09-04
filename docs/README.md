# 📚 Documentación - DistributedProcessing

Bienvenido a la carpeta de documentación del proyecto **DistributedProcessing**. Aquí encontrarás toda la información arquitectónica y de diseño del sistema.

## 📋 Contenidos

### 📐 Diagramas Arquitectónicos

- **`DiagramaDeComponentes.drawio`**: Diagrama de componentes del sistema
  - Muestra los componentes principales y sus interacciones
  - Incluye servicios, APIs y dependencias

- **`DiagramaDeDespliegue.drawio`**: Diagrama de despliegue
  - Estructura física del deployment
  - Nodos, contenedores y distribución de recursos
  - Arquitectura en producción

- **`DiagramaDeSecuencia.drawio`**: Diagrama de secuencia
  - Flujos de comunicación entre componentes
  - Interacciones temporales entre servicios
  - Patrones de llamadas entre procesos

### 📄 Documentación Técnica

Los documentos técnicos se escriben en [Typst](https://typst.app) (`.typ`) y se compilan automáticamente a PDF por un GitHub Action al hacer push a `main`, publicándose en `RenderedDocuments/` junto con los diagramas `.drawio` exportados a PNG.

- **`01-alcance.typ`**: Alcance del proyecto
  - Problema que resuelve el sistema y no-objetivos
  - Restricciones de red y hardware disponible por nodo
  - Volumen de datos esperado y métricas de éxito

- **`02-arquitectura.typ`**: Decisiones técnicas y trade-offs
  - Justificación de usar **Go**
  - Justificación de usar **gRPC**
  - Justificación de usar **Docker**
  - Matriz de evaluación de alternativas
  - Arquitectura general del sistema

- **`03-esquema-datos.typ`**: Esquema de datos MapReduce
  - Contratos de `InputChunk`, `MapOutput`, `ReduceInput` y `FinalResult`
  - Definición formal en Protocol Buffers

## 🏗️ Stack Tecnológico

```
┌─────────────────────────────────────────┐
│      Lenguaje: Go 1.26.1                │
├─────────────────────────────────────────┤
│      Protocolo: gRPC + Protocol Buffers │
├─────────────────────────────────────────┤
│      Contenedorización: Docker          │
├─────────────────────────────────────────┤
│      Orquestación: Kubernetes (roadmap) │
└─────────────────────────────────────────┘
```

## ⚡ Quick Facts

| Aspecto | Descripción |
|--------|------------|
| **Lenguaje** | Go (compiled, statically typed) |
| **Protocolo RPC** | gRPC (HTTP/2 + Protocol Buffers) |
| **Concurrencia** | Goroutines (lightweight threads) |
| **Contenedorización** | Docker (multi-stage builds) |
| **Orquestación** | Docker Compose / Kubernetes |

## 📝 Notas para Desarrolladores

- Todos los diagramas están en formato `.drawio` (compatible con [Draw.io](https://draw.io) y [Diagrams.net](https://app.diagrams.net))
- La documentación técnica está en formato Typst (`.typ`); no se versionan los PDF ni PNG generados a mano, esos se publican automáticamente en `RenderedDocuments/` vía GitHub Actions
- La documentación se actualiza conforme evoluciona la arquitectura
- Las decisiones técnicas están racionalizadas en `02-arquitectura.typ` para futuras referencias

## 🔗 Enlaces Útiles

- [Go Documentation](https://golang.org/doc/)
- [gRPC Official Guide](https://grpc.io/docs/languages/go/)
- [Docker Documentation](https://docs.docker.com/)
- [Diagrams.net - Editor en línea](https://app.diagrams.net/)

---

**Última actualización**: 31/08/2026
