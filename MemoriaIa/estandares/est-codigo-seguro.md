---
tipo: estandar
actualizado: 2026-11-03
---
# Lineamiento de código seguro de Alfa (GTC-GDD-003, versión 2)

Fuente: `Docs\02-estandares-banco\versionamiento-seguro\LINEAMIENTO PARA EL VERSIONAMIENTO DEL CODIGO SEGURO ALFALV2.pdf` (8 págs.). Pese al nombre del archivo, trata de **análisis de código seguro con SonarQube**, no de versionamiento.

## Reglas verificables
- **Herramienta oficial:** SonarQube (análisis estático: bugs, vulnerabilidades, code smells, duplicados, complejidad, cobertura).
- **Quality Gate de referencia:** sin evidencias bloqueantes; **cobertura de código nuevo > 80 %**.
- **Proceso interno (5.1):**
  1. El líder técnico entrega la rama y el reporte SonarQube **inicial** (línea base).
  2. El desarrollador hace push y compara su reporte con la línea base: no debe agregar errores.
  3. Solo con cero errores nuevos se hace PR hacia QA, adjuntando la comparación.
- **Proveedores (5.2):** deben entregar reporte de SonarQube (u otra herramienta) **al inicio y al final** del ajuste; un responsable de Alfa da el aval.
- **Control para producción (6):** la evidencia del análisis es requisito del paso a producción y la valida el **Comité de Cambios y el Oficial de Seguridad de la Información**.
- **Excepción (7):** solo archivos `.dsx` (DataStage).

## Relevancia
- Aplica a todo el código que toquemos (lambdas del paso 3, validador de la tarea 3). Antes de modificar: pedir la línea base SonarQube del repositorio (ver [[pendientes]]).
- Si trabajamos como proveedor externo, aplica 5.2: reporte inicial y final obligatorio.
- Cobertura > 80 % sobre código nuevo exige pruebas unitarias para las lambdas que se unifiquen (tarea 2).
