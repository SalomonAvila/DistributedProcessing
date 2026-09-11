# Datasets crudos (SECOP II)

Esta carpeta es el destino local de los CSV descargados de datos.gov.co. No se versionan en git (20-40 GB entre ambos datasets) — ver `.gitignore`.

## Procesos de Contratación

- Fuente: https://www.datos.gov.co/Estad-sticas-Nacionales/SECOP-II-Procesos-de-Contrataci-n/p6dx-8zbt/about_data
- Destino: `data/raw/procesos-de-contratacion/`
- Columnas relevantes esperadas por el chunker (`src/pkg/chunker/chunker.go`): `id_del_proceso`, `nit_entidad`, `entidad`, `proveedores_invitados`, `proveedores_unicos_con`, `modalidad_de_contratacion`, `estado_del_procedimiento`.

## Contratos Electrónicos

- Fuente: https://www.datos.gov.co/Estad-sticas-Nacionales/SECOP-II-Contratos-Electr-nicos/jbjy-vk9h/about_data
- Destino: `data/raw/contratos-electronicos/`
- Columnas relevantes esperadas por el chunker: `proceso_de_compra`, `nit_entidad`, `nombre_entidad`, `documento_proveedor`, `proveedor_adjudicado`, `valor_del_contrato`, `codigo_de_categoria_principal`, `fecha_de_firma`.

Cualquier CSV que se deje dentro de `data/raw/**` queda ignorado por git automáticamente.
