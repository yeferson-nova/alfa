# Malla 2 de PCR: Primas Cedidas

Parte de [PCR](../README.md). Certifica Operaciones. Consulta base: `M2_PRIMAS_CEDIDAS.sql` (`Docs\05-arquitectura\Reuniones\Extraccion\`).

## Estado
**Pendiente.** Manuel construye la versión base de esta malla. Cuando exista, entra a este plan con los mismos 3 objetivos: extracción en 1,5 horas como máximo, código modular y limpio, y observabilidad en 3 niveles.

## Lo que ya se sabe
Usa 17 tablas de Acsel-e: 9 ya las extrae la malla 1 y faltan 8 (`REASEGURADOR`, `REINSURANCEOPERATION` y sus tablas `COMPONENT`, `DETAIL`, `GROUPINFO` y `PROPERTY`, `RI_PARTICIPATION` y `RI_REINSURANCECONTRACT`). También usa las tablas `EM_*` que deja la malla 1.

## Validaciones
Cuando empiece, se crea `validaciones\scripts\` y `validaciones\salidas\`, igual que en [la malla 1](../01-emisiones/README.md).
