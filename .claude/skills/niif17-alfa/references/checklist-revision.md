# Lista de revisión (PR, entregable o incidente)

Marcar cada punto como cumple, no cumple (con evidencia) o no aplica. Lo que no cumple se convierte en hallazgo con ID.

## Código
- [ ] Nombres de repo, paquete, recursos y rol según V3 y HU-DO.
- [ ] Estructura hexagonal; la prueba de arquitectura pasa; el handler o el script principal no tiene lógica.
- [ ] Sin valores quemados, sin defaults de otra malla o ambiente; configuración validada al arrancar.
- [ ] Sin secretos ni datos personales en código, config, logs, pruebas o repo.
- [ ] Errores con taxonomía; ninguno silenciado; reintentos limitados y destino de fallo.
- [ ] Logs JSON con `ejecucion_id`, eventos del catálogo, métricas EMF.
- [ ] Idempotente: el mismo evento dos veces no repite efectos; transiciones de estado condicionales.
- [ ] Cobertura de código nuevo > 80 %, Quality Gate de SonarQube en verde contra la línea base.
- [ ] README con contrato, argumentos o variables, permisos mínimos, despliegue y vuelta atrás.
- [ ] Entrega en un zip con el nombre del recurso (`niif-<amb>-<nombre>.zip`) con `src/lambda_function.py` (o el script del job), `test/` y README; DevOps hace el commit. Nada sensible en el zip (evidencia solo en `EvidenciasAws\`).

## Datos (jobs de Glue)
- [ ] Columnas explícitas, filtro por periodo, particiones.
- [ ] Compuerta de calidad antes de escribir; regla bloqueante detiene.
- [ ] Cuadre `entra = sale + cuarentena` registrado y con alarma.
- [ ] Modo de carga (completa o incremental) explícito y coherente con el SCD.
- [ ] Escritura reversible (versión anterior recuperable).
- [ ] Importes en DECIMAL con precisión, escala y redondeo declarados; totales calculados una vez; cuadre de sumas de valores entre capas.
- [ ] Impacto declarado en tiempo, DPU-horas y bytes escaneados de Athena (medido o con plan de medición).
- [ ] Athena con workgroup, tope, tiempo máximo e intentos; "sin datos" contado sin encabezado.
- [ ] Timeout y concurrencia del job según la realidad.

## Notificación y certificación
- [ ] No se envía con resumen, totales o detalle vacíos.
- [ ] HTML escapado; destinatarios de metadata con lista permitida.
- [ ] Enlaces: confirmación + POST, token de un uso con expiración, identidad federada, auditoría.
- [ ] URL prefirmada corta, bucket privado, KMS.
- [ ] Lo certificado es lo transportado (checksum).
- [ ] El fallo de cualquier paso llega a soporte por monitoreo.

## Análisis de un incidente
1. Hechos comprobados (con la evidencia y cómo se comprobó) separados de hipótesis.
2. Cadena causal paso a paso, con archivo y línea.
3. Validación por script o consulta (PASS/FAIL) de cada afirmación.
4. Qué no está comprobado y qué evidencia falta (con pasos de consola o CloudShell de solo lectura).
5. Acciones: contención inmediata, corrección de causa, prevención; distinguir parche de solución.
6. Riesgo en las demás mallas que comparten el componente.
7. Registrar en memoria (`brechas\`), en `[[pendientes]]` y `[[preguntas-abiertas]]`.
Modelo: `[[inprod-primas-arl-202606]]`.

## Severidad
**Bloqueante:** puede certificar o transportar una cifra errónea, perder datos o exponer datos personales. **Alto:** sin evidencia, sin alerta o con costo sin tope. **Medio:** rendimiento, mantenibilidad, deuda que no altera cifras.
