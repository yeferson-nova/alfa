---
tipo: contexto
actualizado: 2026-11-03
---
# Marco normativo y estándares aplicables

Decisión del usuario: el marco se toma de lo entregado, no de supuestos. Entidad: **Seguros Alfa S.A. y Seguros de Vida Alfa S.A.** (aseguradora; los documentos la nombran así). Todo sobre AWS, región us-east-1.

## Normas externas citadas en los documentos entregados
| Norma | Para qué la invocan | Dónde |
|---|---|---|
| **Ley 1581 de 2012** | Datos personales: finalidad, circulación restringida, veracidad (no completar con valores ficticios), seguridad, minimización, no usar datos reales fuera de producción | Los 6 libros de HUs |
| **Decreto 1377 de 2013** (compilado en el Decreto 1074 de 2015) | Reglamento de la Ley 1581: autorización y base legal | [[hu-seguridad-acceso]], [[hu-remediacion]] |
| **Circular Básica Contable y Financiera (SFC)** | Trazabilidad, demostrabilidad y explicabilidad de las cifras del cierre | [[hu-gobierno-ingestas]], [[hu-linaje]], [[hu-trazabilidad]], [[hu-remediacion]] |
| **Circular Básica Jurídica (SFC)** | Seguridad de la información, trazabilidad y conservación | [[hu-seguridad-acceso]], [[hu-devops]] |

Los documentos **no citan** capítulos ni circulares externas específicas (como la CE 007/2018 o la CE 005/2019). No se agregan, por la decisión del usuario.

## Requisitos derivados que se repiten
- **Retención de evidencia: 7 años** (Landing y bitácoras, [[hu-remediacion]] RM-D09).
- Las notificaciones **no llevan datos personales** ([[hu-remediacion]]).
- No se usan datos personales reales fuera de producción.
- Las cifras publicadas al negocio deben poder demostrarse hasta el cierre contable ([[hu-trazabilidad]] HU-TR-04).

## Lineamientos internos de Alfa
Entregados:
- [[est-etl]] GTC-GDD-010
- [[est-datastage]] v6
- [[est-versionamiento-codigo]] V3
- [[est-codigo-seguro]] GTC-GDD-003
- [[est-databrew]] (manual)
- [[est-nuevos-cargues]] (manual)

Citados pero **no entregados**:
- Lineamiento de estrategias de CI/CD V1.
- Lineamiento de IaC con Terraform GTC-GDD-004.
- Modelo de Seguridad de Datos de la Gerencia de Analítica.
- Documentos técnicos 01, 02 y 03 de los temas transversales.

Ver [[preguntas-abiertas]].

## Estándares transversales (HUs, "aplicación obligatoria")
- [[hu-gobierno-ingestas]]
- [[hu-linaje]]
- [[hu-trazabilidad]]
- [[hu-devops]]
- [[hu-remediacion]]
- [[hu-seguridad-acceso]]
