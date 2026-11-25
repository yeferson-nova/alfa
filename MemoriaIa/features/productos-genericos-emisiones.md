---
tipo: feature
estado: vigente
actualizado: 2026-11-25
---
# Productos genéricos en Emisiones (piloto Pymes)

> **Propuesta (2026-11-25), sin aprobar:** que un producto nuevo o retirado entre y salga de Emisiones como dato (catálogo y mapa de conceptos en AWS), sin editar consultas ni código. Pymes (55139) es el piloto que definió negocio.

## Origen
- Reunión del 2026-11-24 (Manuel Machado): Pymes se dejó por fuera a propósito como caso piloto, decisión de Víctor y Jorge. El objetivo es una extracción que tome los productos nuevos o retirados de forma automática; los productos nuevos pueden traer tablas o atributos nuevos.
- Falta una reunión para definir quién mantiene la lista de productos. Detalle en [[inprod-pcr-emisiones-202606]].

## Restricciones (usuario, 2026-11-25)
- **No se toca la Oracle ACSELE**: nada se crea ni se cambia ahí.
- **No hay base espejo de la Oracle.** El Bronce sigue siendo lo de hoy: tablas configuradas, filas del periodo y de la compañía, en archivos por corrida.

## Problema: el producto está escrito a mano en tres lugares
| Dónde | Efecto |
|---|---|
| `IN (...)` en ~40 consultas, repetido por malla (`config_extraccion_<caso>.json`, `cnf-mallas`) | Listas distintas entre casos en tablas `COMUN` → Pymes sin póliza ([[auditoria-extraccion-2026-11]], ARQ-67) |
| `PRODUCTS` en `silver_generales.py` | Hay que editar código por producto (ARQ-65) |
| Listas de conceptos en `silver_generales.py` | Conceptos nuevos se pierden (PCE-1b, PCE-2) |

## Propuesta (4 piezas)
1. **Catálogo de productos en AWS** (DynamoDB o JSON en S3 versionado en Git), mantenido por el área:
   - código, nombre, compañía y caso;
   - estado: `detectado`, `piloto`, `activo` o `retirado`;
   - vigencia.
2. **Lista de productos desde el catálogo.** La configuradora la arma una vez para todas las mallas y todas las consultas. La Oracle se sigue leyendo igual que hoy: periodo, compañía y productos. Base existente: el bloque `clasificacion_caso` de la configuración.
3. **Mapa de conceptos en AWS:** concepto → métrica (prima, comisión, recaudo, IVA) y signo. Por defecto aplica la regla por sufijo del Silver actual (`DEV` y `ANU` restan, `REHAB` suma; `silver_generales.py:739`). El Silver lee el mapa en lugar de listas en el código.
4. **Controles que avisan, nunca pierden en silencio:**
   - producto con movimientos que no está en el catálogo (se compara contra `PRODUCT`/`PREPRODUCT`, que ya se extraen);
   - concepto con monto sin mapear;
   - movimientos sin póliza;
   - columna o tabla nueva frente a la corrida anterior;
   - cuadre Oracle frente a Bronce por producto y concepto.

## Tablas propias de producto
- Son tablas de la Oracle que solo usa un producto: `PO*`, `UR*` y `OA*`. Ejemplos: `POMIHOGARSEGURO`, `URMASCOTAS`, `OAINCTERRDEUDORES`; ya se extraen en Generales.
- La lectura de los prefijos (póliza, unidad de riesgo, objeto asegurado) es **deducción por el nombre**.
- Propuesta: un **perfil de producto** en AWS que diga qué tabla y columna de origen alimenta cada campo del modelo común del Silver.
- Pymes no tiene tablas propias en la configuración. No se sabe si existen en la Oracle: se puede averiguar con una consulta de solo lectura a `ALL_TABLES`, si se permite; si no, las declara el área en el catálogo.

## Piloto Pymes
1. Catálogo y mapa de conceptos solo con Pymes (69 conceptos de Q5).
2. Extracción sin `IN` escrito a mano.
3. El Silver toma productos y conceptos de las tablas.
4. Criterio de éxito, igual al RRC: prima 56.404.532,99, comisión de recaudo 11.280.902,12, IVA de recaudo 2.143.371,37.
5. Probar los controles quitando un concepto del mapa.

Si sale bien, se migran los demás productos y se eliminan las listas del código. Debe ir junto con su configuración (`cnf-mallas`, `cfg-datalake-niif-config-s3`, configuradora). Ver [[modularizacion-extraccion]] y [[preferencias-usuario]].

## Decisiones de negocio pendientes
- Quién mantiene el catálogo: el área, o se arma desde ACSELE con aprobación del área.
- Quién aprueba el mapa de conceptos.
- Ver [[preguntas-abiertas]].
