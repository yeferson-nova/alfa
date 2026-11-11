# Seguridad, código seguro y cumplimiento

Marco: solo lo entregado (`[[marco-normativo]]`). No agregar circulares que los documentos no citan.

## Normas y lineamientos que aplican
| Fuente | Qué exige al código | Nota |
|---|---|---|
| Ley 1581 de 2012 y Decreto 1377 de 2013 | Finalidad, minimización, veracidad (no inventar datos), seguridad, sin datos reales fuera de prod | `[[hu-seguridad-acceso]]`, `[[hu-remediacion]]` |
| Circular Básica Contable y Financiera (SFC) | Cifras trazables y demostrables hasta el cierre | `[[hu-trazabilidad]]` |
| Circular Básica Jurídica (SFC) | Seguridad de la información, trazabilidad, conservación | `[[hu-seguridad-acceso]]`, `[[hu-devops]]` |
| Código seguro GTC-GDD-003 | SonarQube, cero issues nuevos frente a la línea base, cobertura de código nuevo > 80 %, evidencia ante Comité de Cambios y Oficial de Seguridad | `[[est-codigo-seguro]]` |
| Versionamiento V3 | Repos, ramas, commits con ticket, PR con revisores, sin código quemado | `[[est-versionamiento-codigo]]` |
| HU DevOps | IaC, secretos en Secrets Manager + KMS, DevSecOps bloqueante, rollback | `[[hu-devops]]` |

## Reglas de código seguro
1. **Secretos:** nunca en código, `config/`, variables en texto plano, logs ni repo. Secrets Manager o Glue Connection; KMS por dominio.
2. **Mínimo privilegio:** un rol por lambda o job, acciones y recursos explícitos, sin `*` en recursos ni en políticas de invocación. Cada identidad no humana tiene dueño, alcance y recertificación (HU-SG-21).
3. **Entrada no confiable:** validar tipos y rangos en el borde; escapar todo lo que va a HTML; parametrizar SQL de Athena (consultas desde metadata aprobada, valores con parámetros, nunca concatenando texto del evento).
4. **Datos personales:** no en logs, correos, nombres de archivo, métricas ni pruebas. Datos de prueba ficticios. Seudonimización con sal en Secrets Manager donde el estándar lo pida.
5. **Cifrado:** S3, DynamoDB, resultados de Athena y colas con KMS; TLS en todo tránsito.
6. **Acceso a datos por Lake Formation:** el permiso efectivo es el mismo por cualquier ruta; una lambda o URL prefirmada no puede ser un atajo.
7. **Dependencias:** solo las necesarias, con versión fija; revisar vulnerabilidades en el pipeline.
8. **Fallar cerrado:** si falta configuración, permiso o identidad, se detiene; nunca se degrada a un default inseguro o de otro ambiente.
9. **Auditoría:** toda decisión humana y todo cambio de configuración deja registro (quién, cuándo, qué, versión). Retención de evidencia: 7 años.

## Repositorio (privado desde 2026-11-09; lo anterior quedó en el historial)
El repo `alfa` fue público hasta el 2026-11-09. Se trata como si siguiera expuesto. Antes de escribir en él cualquier archivo:
- Nada de evidencia de prod, correos de personas, IDs de Cognito, números de cuenta, IPs, nombres de host internos, tokens, URLs prefirmadas ni `.env`.
- Redactar lo sensible o dejarlo en `EvidenciasAws\` (ignorada por el `.gitignore` de la raíz; solo se versiona su `LEEME.md`).
- Preguntar al usuario antes de subir evidencia.

## Brechas: cómo analizar
Todo análisis revisa tres ejes contra `Docs\02-estandares-banco\` y las HU:
- **Seguridad (`SEG-xx`):** identidad, permisos, secretos, cifrado, datos personales, endpoints, correo.
- **Costos (`COS-xx`):** Athena (workgroup, tope de escaneo, columnas, particiones, resultados), DPU-horas de Glue, timeouts, crawlers, logs, almacenamiento sin ciclo de vida.
- **Arquitectura (`ARQ-xx`):** contratos, concurrencia, idempotencia, compuertas, acoplamiento, duplicación, IaC.
- **Cumplimiento (`CUM-xx`):** trazabilidad, retención, demostrabilidad de cifras, Ley 1581.
Cada brecha: ID, severidad, evidencia (archivo y línea o consulta), norma que incumple, corrección propuesta, tarea. Mismo ID en `Docs\06-brechas\` y en la nota de memoria.
