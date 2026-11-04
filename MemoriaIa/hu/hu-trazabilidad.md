---
tipo: hu
actualizado: 2026-11-03
---
# HUs Tema 03: Trazabilidad y Auditoría (HU-TR-01 a 05)

Fuente: `Docs\03-historias-usuario\03_HUs_Tecnicas_Trazabilidad_Auditoria.xlsx` (ago-2026). Estándar transversal obligatorio.

## Reglas
- **HU-TR-01:** ninguna transición de capa ocurre sin traza; tabla `aud_ejecucion_por_capa` con el `ejecucion_id` común.
- **HU-TR-02, compuerta de calidad:** **se valida antes de escribir en la capa destino; si falla, se detiene y se alerta** en lugar de escribir datos parciales. Las reglas se declaran por objeto y transición, con tipo (estructura, calidad, negocio, conciliación), versión, umbral y severidad. El resultado va a `aud_resultado_reglas_calidad`. Ningún registro se descarta sin dejar traza.
- **HU-TR-03:** cuarentena `cuarentena_<capa>_<objeto>` y control `aud_control_cuarentena` (volumen, causa, responsable, estado).
- **HU-TR-04, cuadre entre capas:** cada transición verifica su ecuación (entra = sale + cuarentena) y **una diferencia bloquea el avance**. La cifra publicada debe ser demostrable hasta el cierre contable. El formato de la evidencia se acuerda con Contabilidad y Actuaría.
- **HU-TR-05:** tablero, indicadores y alertas con responsable y flujo de atención.
- **Otras tablas:** `aud_acceso_de_consumo` y `aud_eventos_de_cambio`. Prefijo `aud_` y nombres completos, sin abreviaturas.

## Decisiones abiertas
- **T-01:** umbrales de rechazo.
- **T-02:** retención de cuarentena (con datos personales).
- **T-03:** notificación y escalamiento.
- **T-04:** registro de acceso.
- **T-05:** catálogo corporativo de reglas.

## Relevancia, la más alta para la tarea 3 ([[tareas-aws]])
- La Lambda de Validación de [[diseno-propuesto]] es, en la práctica, una **compuerta HU-TR-02 + cuadre HU-TR-04** para las cifras de reserva matemática. Debe:
  - bloquear la salida a S3 si falla;
  - registrar el resultado por regla;
  - enviar a cuarentena lo rechazado.
- El diseño ya propuesto coincide con el estándar. Falta adoptar sus nombres de tablas y el registro de resultados.
- El formato de evidencia "acordado con Contabilidad y Actuaría" encaja con la definición funcional pendiente ([[preguntas-abiertas]]).
