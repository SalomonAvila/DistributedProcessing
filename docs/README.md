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

- **`02-arquitectura.md`**: Decisiones técnicas y trade-offs
  - Justificación de usar **Go**
  - Justificación de usar **gRPC**
  - Justificación de usar **Docker**
  - Matriz de evaluación de alternativas
  - Arquitectura general del sistema

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
- La documentación se actualiza conforme evoluciona la arquitectura
- Las decisiones técnicas están rationalizadas en `02-arquitectura.md` para futuras referencias

## 🔗 Enlaces Útiles

- [Go Documentation](https://golang.org/doc/)
- [gRPC Official Guide](https://grpc.io/docs/languages/go/)
- [Docker Documentation](https://docs.docker.com/)
- [Diagrams.net - Editor en línea](https://app.diagrams.net/)

---

**Última actualización**: 31/08/2026
