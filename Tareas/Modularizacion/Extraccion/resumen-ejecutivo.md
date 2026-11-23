# Resumen ejecutivo: mejora de la extracción NIIF 17

**Periodo:** 23 de noviembre de 2026 a 8 de enero de 2027 (7 semanas, 4 sprints de 2 semanas). Holgura: 11 a 15 de enero.
**Alcance:** los dos procesos que leen las fuentes Oracle y cargan la capa Bronce:
- **Proceso de extracción estándar:** 6 mallas de ALFAPRD (ARL, Rentas, Endosos, Reserva Matemática ARP y RVI, Auxilios Funerarios).
- **Proceso de extracción de Emisiones:** Acsel-e (Emisiones, PCR Vida, Generales y Satélites).

**Condición de éxito:** las cifras que entrega cada proceso no cambian. Cada entrega se compara contra la corrida actual con cero diferencias.

## Qué se aborda

| Tema | Qué se hace | Beneficio | Aprobado en QA |
|---|---|---|---|
| 1. Preparación | Medir cómo corre hoy cada proceso y dejar pruebas automáticas que protejan los resultados | Toda mejora se compara contra una línea base real | 27 nov (no pasa por QA) |
| 2. Código modular y deuda técnica | Ordenar el código en módulos con estándares de industria (Python hexagonal). Corregir la configuración fija y los tiempos mal medidos. Dejar una sola versión del proceso de Emisiones | Cambios más rápidos y seguros; agregar tablas o ciclos se hace por configuración, sin desplegar código | 15 dic |
| 3. Observabilidad | Un identificador único por ejecución, de punta a punta, y logs estructurados con conteos y tiempos por paso | Ubicar una falla en minutos y dejar listas las métricas para tableros | 18 dic |
| 4. Control de carga sobre Oracle | Leer por ciclos con un tope de esfuerzo y pausas configurables | Evita saturar la fuente, que es compartida y no tiene réplica de lectura | 24 dic |
| 5. Consultas optimizadas | Leer solo las filas que se necesitan, por tramos de llave, y entregar recomendaciones de índices a Oracle | Menos datos leídos y tiempos estables | 7 ene |
| 6. Cierre | Medición final frente a la línea base y ajuste de recursos de cómputo | Evidencia del ahorro y del tiempo ganado | 8 ene |

## Por qué (situación actual medida en DEV)

- **Se leen tablas completas cuando solo se necesita una parte.** Un ejemplo: se traen 1,2 millones de filas de personas cuando solo se usan las de 7.002 afiliaciones. Rentas relee 22,6 millones de filas en cada corrida.
- **No hay control de carga.** Las lecturas van una tras otra, sin pausa, sobre una sola conexión.
- **El tiempo es impredecible.** Con la misma configuración, ARL tardó entre 4 y 20 minutos y Rentas entre 19 y 45.
- **Las mediciones y el rastreo son incompletos.** Las duraciones registradas no reflejan la lectura real, y no hay un identificador que una los pasos de una misma ejecución.

## Calendario

| Sprint | Fechas | Entrega principal |
|---|---|---|
| S1 | 23 nov a 4 dic | Preparación, código estándar modular y configuración sin valores fijos |
| S2 | 7 a 18 dic | Emisiones modular, observabilidad y control de carga (estándar) |
| S3 | 21 dic a 1 ene | Control de carga (Emisiones) y consultas por tramos (estándar) |
| S4 | 4 a 15 ene | Consultas por tramos (Emisiones), índices y cierre |

Una fecha se cumple cuando el cambio llega a QA y QA lo aprueba. Se supone que QA toma 3 días hábiles por entrega.

## Qué se necesita de otros

| Quién | Qué | Para cuándo |
|---|---|---|
| DevOps | Ticket de Jira y definiciones de arranque | 24 nov |
| DevOps | Parámetro del identificador de ejecución en los orquestadores | 10 dic |
| QA / Oracle | Resultado de las consultas de diccionario (índices y tamaños) | 18 dic |
| Revisores | 2 aprobaciones por PR y promoción a QA | En cada entrega |

## Riesgos y plan de contingencia

- **Si una dependencia se atrasa,** se mueve solo el hito que bloquea.
- **Si el plan completo se atrasa,** se recorta alcance antes de mover el cierre. El control de carga y las consultas por tramos del proceso de Emisiones pasan a una segunda iteración.
- **El proceso de Emisiones tuvo cambios recientes de otra persona.** Se coordina al inicio para no cruzar trabajos.

Detalle: [README](README.md) · [backlog](backlog.md) · [roadmap de cambios](roadmap-cambios.md).
