# Malla 4 de PCR: Primas PCR

Parte de [PCR](../README.md). Certifica Actuaría. Consulta base: `M4_PRIMAS_PCR.sql` (`Docs\05-arquitectura\Reuniones\Extraccion\`).

## Estado
**Pendiente.** Manuel construye la versión base de esta malla. Cuando exista, entra a este plan con los mismos 3 objetivos: extracción en 1,5 horas como máximo, código modular y limpio, y observabilidad en 3 niveles.

## Lo que ya se sabe
Casi no lee Acsel-e: reutiliza `EMISIONES`, `PRIMAS_CEDIDAS` y `REASEGUROS` de las mallas 1 a 3. Solo le falta extraer `PARTICIPACION`, además de `STTS_ROLE` y un delta. Produce `pcr_detalle`, `pcr_totales` y `pcr_comisiones`.

## Validaciones
Cuando empiece, se crea `validaciones\scripts\` y `validaciones\salidas\`, igual que en [la malla 1](../01-emisiones/README.md).
