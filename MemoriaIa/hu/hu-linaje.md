---
tipo: hu
estado: vigente
actualizado: 2026-11-03
---
# HUs Tema 02: Linaje de Datos (HU-LN-01 a 04)

Fuente: `Docs\03-historias-usuario\02_HUs_Tecnicas_Linaje_Datos.xlsx` (ago-2026). Estándar transversal obligatorio. Proyecto: Desmonte de DWH y FUD.

## Reglas
- El grafo de linaje **se construye solo**: cada trabajo, al terminar, emite un evento con lo que leyó, lo que escribió y el mapeo de columnas (OpenLineage, con el conector nativo del catálogo). No hay receptor propio ni mantenimiento manual.
- Linaje **a nivel de columna**, versionado, que arranca en la tabla real del sistema origen (HU-LN-02), no en el lago.
- Cada activo expone junto a su grafo el resultado de calidad y la referencia a la cuarentena (HU-LN-03).
- Ningún activo se retira ni cambia de forma incompatible sin análisis de impacto sobre el grafo (HU-LN-04).
- Nombres del activo tomados del catálogo de gobierno ([[hu-gobierno-ingestas]]), nunca inventados en ejecución.

## Decisiones abiertas
- **L-01:** servicio de catálogo de negocio y su licenciamiento.
- **L-02:** espacio de nombres por ambiente.
- **L-03:** retención del linaje.
- **L-04:** linaje de reportes.
- **L-05:** procedimiento de apagado del DWH.

## Relevancia
Baja para las tareas actuales. Si la Lambda de Validación o la de Transporte del paso 3 escriben datos (salida a S3), deberían emitir su evento de linaje (propuesta, brecha ARQ menor).
