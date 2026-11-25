# Malla 3 de PCR: Reaseguros

Parte de [PCR](../README.md). Certifica Área de Reaseguros. Consulta base: `M3_REASEGUROS.sql` (`Docs\05-arquitectura\Reuniones\Extraccion\`).

## Estado
**Pendiente.** Manuel construye la versión base de esta malla. Cuando exista, entra a este plan con los mismos 3 objetivos: extracción en 1,5 horas como máximo, código modular y limpio, y observabilidad en 3 niveles.

## Lo que ya se sabe
Usa 14 tablas de Acsel-e: 7 ya las extrae la malla 1 y faltan 7 (`EXT_RISKRESERVEALFA`, `FINANCIALPLANDCO`, `PERSONAJURIDICA`, `RI_REINSURANCEGROUP`, `STCA_DOCTYPE`, `STST_SYSTEMPROPERTY` y `TDCALIFICACIONRIESGO`). También usa las tablas `EM_*` y `PC_*` de las mallas 1 y 2.

## Validaciones
Cuando empiece, se crea `validaciones\scripts\` y `validaciones\salidas\`, igual que en [la malla 1](../01-emisiones/README.md).
