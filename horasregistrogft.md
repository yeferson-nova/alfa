# Registro de horas

Campos del formulario "Time Entry". Un apartado por día, del más reciente al más antiguo. El 23 y 24 de noviembre salen de la memoria; del 25 al 30 son una **estimación** según las prioridades del plan de trabajo (ajustar con lo que realmente pase).

## Datos que no cambian
| Campo | Valor |
|---|---|
| Activity | SEGUROS ALFA S.A.-GBAS204F - Célula DataStage y AWS (KE064235-049) (Developer) |
| Time Type | Día hábil, tiempo regular. **Confirmar el nombre exacto de la opción en la lista** |
| Entry Hours | 8 por día (supuesto de jornada completa; **ajustar** si fueron otras horas) |

## Resumen
| Fecha | Día | Horas | Tipo | Tema |
|---|---|---|---|---|
| [30/11/2026](#dia-2026-11-30) | Lunes | 8 | Estimado | Preparación del cambio de PCR Emisiones para la primera semana de diciembre y optimizaciones de cedidas-riesgo |
| [27/11/2026](#dia-2026-11-27) | Viernes | 8 | Estimado | Optimizaciones de PCR Emisiones y avance de cedidas-riesgo |
| [26/11/2026](#dia-2026-11-26) | Jueves | 8 | Estimado | PaP de PCR Emisiones y atención de lo que salga |
| [25/11/2026](#dia-2026-11-25) | Miércoles | 8 | Estimado | Certificación de cifras de PCR Emisiones y soporte a defectos |
| [24/11/2026](#dia-2026-11-24) | Martes | 8 | Real | PCR Emisiones: validación de cifras, evidencias y correcciones antes de certificar |
| [23/11/2026](#dia-2026-11-23) | Lunes | 8 | Real | PCR Emisiones: diagnóstico de cifras de PRD frente a QA, pruebas en AWS y avance de la extracción |

---

<a id="dia-2026-11-30"></a>
## 30/11/2026 (lunes) — estimado

| Campo | Valor |
|---|---|
| Start Date | 30/11/2026 |
| End Date | 30/11/2026 |
| Time Type | Día hábil, tiempo regular (lunes, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

PCR Emisiones: dejé lista la entrega de las optimizaciones para el cambio de la primera semana de diciembre, con pruebas y guía para DevOps. Continué las optimizaciones de cedidas-riesgo.

---

<a id="dia-2026-11-27"></a>
## 27/11/2026 (viernes) — estimado

| Campo | Valor |
|---|---|
| Start Date | 27/11/2026 |
| End Date | 27/11/2026 |
| Time Type | Día hábil, tiempo regular (viernes, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

Trabajé en las optimizaciones de PCR Emisiones que van en el cambio de diciembre y avancé las de cedidas-riesgo. Di soporte a fallos de otros casos de uso para estabilizarlos.

---

<a id="dia-2026-11-26"></a>
## 26/11/2026 (jueves) — estimado

| Campo | Valor |
|---|---|
| Start Date | 26/11/2026 |
| End Date | 26/11/2026 |
| Time Type | Día hábil, tiempo regular (jueves, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

Acompañé el paso a producción (PaP) de PCR Emisiones y revisé las cifras después del despliegue. Atendí los defectos que aparecieron y empecé a ordenar las optimizaciones del siguiente cambio.

---

<a id="dia-2026-11-25"></a>
## 25/11/2026 (miércoles) — estimado

| Campo | Valor |
|---|---|
| Start Date | 25/11/2026 |
| End Date | 25/11/2026 |
| Time Type | Día hábil, tiempo regular (miércoles, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

Certificación de cifras de PCR Emisiones con negocio: apoyé la revisión de los valores y corregí los defectos que aparecieron. Dejé lista la información para el paso a producción del día siguiente.

---

<a id="dia-2026-11-24"></a>
## 24/11/2026 (martes)

| Campo | Valor |
|---|---|
| Start Date | 24/11/2026 |
| End Date | 24/11/2026 |
| Time Type | Día hábil, tiempo regular (martes, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

PCR Emisiones: validé las cifras con consultas a Oracle y a AWS, armé las evidencias y encontré que el núcleo compartido de la extracción dejaba fuera Pymes. Preparé el correo de entrega de evidencias.

---

<a id="dia-2026-11-23"></a>
## 23/11/2026 (lunes)

| Campo | Valor |
|---|---|
| Start Date | 23/11/2026 |
| End Date | 23/11/2026 |
| Time Type | Día hábil, tiempo regular (lunes, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

PCR Emisiones: investigué por qué PRD trae cifras distintas a QA. Armé scripts de diagnóstico, los corrí en DEV y QA, comparé contra los Excel de negocio y documenté las fallas. También avancé el análisis de la extracción.

### Detalle de lo trabajado
- Diagnóstico por ambiente y comparador por capas; resultados de DEV y QA frente a Oracle.
- Cuadre de montos contra el Excel de negocio y fallas halladas en Generales.
- Brechas ARQ-61 a ARQ-66 de la extracción.
