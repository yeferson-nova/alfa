# Registro de horas

Campos del formulario "Time Entry". Un apartado por día, del más reciente al más antiguo.

## Datos que no cambian
| Campo | Valor |
|---|---|
| Activity | SEGUROS ALFA S.A.-GBAS204F - Célula DataStage y AWS (KE064235-049) (Developer) |
| Time Type | Día hábil, tiempo regular. **Confirmar el nombre exacto de la opción en la lista** |
| Entry Hours | 8 por día (supuesto de jornada completa; **ajustar** si fueron otras horas) |

## Resumen
| Fecha | Día | Horas | Tema |
|---|---|---|---|
| [20/11/2026](#dia-2026-11-20) | Viernes | 8 | Modularización de la extracción: reunión con Víctor, consultas base de Oracle, diseño de lectura por rangos y roadmap de cambios |
| [19/11/2026](#dia-2026-11-19) | Jueves | 8 | Informe de observabilidad (logs, métricas y trazas) y medición de tiempos de Primas Rentas |
| [18/11/2026](#dia-2026-11-18) | Miércoles | 8 | Pruebas del fix de Primas Rentas en DEV y revisión de los logs de Primas ARL en QA |
| [17/11/2026](#dia-2026-11-17) | Martes | 8 | Fix del SCD por periodo de Primas ARL: plan, script y pruebas en DEV |

---

<a id="dia-2026-11-20"></a>
## 20/11/2026 (viernes)

| Campo | Valor |
|---|---|
| Start Date | 20/11/2026 |
| End Date | 20/11/2026 |
| Time Type | Día hábil, tiempo regular (viernes, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

Modularización de la extracción: revisé con Víctor cómo se leen las tablas de Oracle, dejé las consultas base y propuse leer por rangos de llave. Armé el roadmap de cambios y documenté las brechas nuevas de la extracción.

### Detalle de lo trabajado
- Reunión con Víctor sobre la extracción y consultas base M1 a M4 como referencia de resultados.
- Tarea de modularización con el componente de extracción, alcance y entregable definidos.
- Diseño de lectura por rangos de llave con semijoin, y roadmap de cambios.
- Brechas ARQ-57 a ARQ-60 y COS-11; revisión del código de los extractores.

---

<a id="dia-2026-11-19"></a>
## 19/11/2026 (jueves)

| Campo | Valor |
|---|---|
| Start Date | 19/11/2026 |
| End Date | 19/11/2026 |
| Time Type | Día hábil, tiempo regular (jueves, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

Observabilidad: armé el informe técnico con logs, métricas y trazas, el contrato de log y el recorrido del ID de ejecución, y actualicé el diagrama. También medí tiempos de Primas Rentas con un script y apunté lo hablado con Víctor.

### Detalle de lo trabajado
- Informe de observabilidad (fases 0 a 6) y brechas ARQ-51 a ARQ-56, SEG-17 y COS-10.
- Diagrama de la propuesta con observabilidad agregada.
- Script para medir tiempos de Primas Rentas, con ajustes.
- Reunión con Víctor: enlace corto, modelo de identidades y segregación de eventos.

---

<a id="dia-2026-11-18"></a>
## 18/11/2026 (miércoles)

| Campo | Valor |
|---|---|
| Start Date | 18/11/2026 |
| End Date | 18/11/2026 |
| Time Type | Día hábil, tiempo regular (miércoles, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

Probé en DEV el fix de Primas Rentas en la Silver con un script y su guía, incluyendo la Step Function y la lambda, y documenté el resultado. Revisé los logs de las pruebas de Primas ARL en QA y actualicé la memoria.

### Detalle de lo trabajado
- Script y guía de pruebas de la Silver de Rentas en DEV, con resultados.
- Logs de QA de Primas ARL (varias corridas) analizados.
- Entrega de SES con HTML cerrada en QA y en paso a PRD.

---

<a id="dia-2026-11-17"></a>
## 17/11/2026 (martes)

| Campo | Valor |
|---|---|
| Start Date | 17/11/2026 |
| End Date | 17/11/2026 |
| Time Type | Día hábil, tiempo regular (martes, no es festivo en Colombia) |
| Entry Hours | 8 (supuesto de jornada completa; **ajustar** si fueron otras horas) |

### Notes (texto para pegar en el campo)

Fix del SCD por periodo de Primas ARL: dejé el plan, escribí el script del paso 4 y corrí las pruebas en DEV. Recuperé el mockup HTML del correo y actualicé la memoria y los estándares de PR.

### Detalle de lo trabajado
- Plan del fix y script del paso 4 de Primas ARL.
- Pruebas en DEV del SCD con freno por periodo.
- Mockup HTML recuperado; estándar de pull requests documentado.
