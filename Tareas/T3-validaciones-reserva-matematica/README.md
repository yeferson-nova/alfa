# T3. Validaciones de consistencia en reserva matemática

**Estado:** la definición funcional está en curso (la hace el negocio). **Depende de:** T2 (modelo de lambdas y configuración) y de esa definición.

## Qué se pidió
Generar validaciones que detecten errores en la consistencia de los datos procesados de reserva matemática.

## El problema
La reserva matemática es el pasivo que la aseguradora mantiene para cubrir sus obligaciones futuras con los asegurados; sus cifras alimentan el cálculo actuarial y el cierre contable. Hoy el paso 3 envía las cifras a certificar y luego las transporta sin comprobar que sean consistentes. Eso tiene tres consecuencias:
- quien certifica solo ve totales y no puede detectar errores internos, como registros perdidos, duplicados o valores imposibles;
- un error certificado llega a las herramientas actuariales (FIS, Prophet) y al cierre, donde corregirlo cuesta más;
- la normativa de la SFC exige demostrar el origen y la transformación de las cifras del cierre; sin validaciones registradas, esa trazabilidad no se puede probar.

## Qué exigen los estándares
| Requisito | Fuente |
|---|---|
| Validar **antes** de escribir en el destino. Si falla, se detiene y se alerta en vez de entregar datos parciales. | HU-TR-02 (compuerta de calidad) |
| Las reglas se declaran por objeto, con tipo (estructura, calidad, negocio o conciliación), versión, umbral y severidad. Cada resultado queda registrado. | HU-TR-02 |
| **Cuadre:** lo que entra = lo que sale + lo rechazado. Una diferencia bloquea el avance. La cifra publicada debe poder demostrarse hasta el cierre contable. | HU-TR-04 |
| Lo rechazado no se descarta: va a una tabla de cuarentena con su motivo. | HU-TR-03 |
| Comparar el volumen extraído con el cargado, y verificar la integridad antes y después de cada proceso. | Estándar ETL 6.2 |
| Validar que el archivo exista, tenga la estructura correcta y no esté vacío; si no cumple, notificar sin abortar. | Estándar DataStage 8.9 |
| No completar datos faltantes con valores ficticios (principio de veracidad). | Ley 1581, citada en las HUs |

## Propuesta
- **Lambda de Validación** nueva en el paso 3, entre Consolidación y Transporte (`Docs\05-arquitectura\aws_pasos_2_y_3.drawio`).
- **Reglas:** cada regla es un validador independiente, declarado en el gestor de configuración (por eso T2 va primero). Agregar una regla no requiere cambiar código.
- **Si una regla bloqueante falla:**
  1. no se transporta nada a S3 de salida;
  2. se notifica (escenario de T1);
  3. el resultado aparece en el correo de certificación, para que quien certifica vea más que los totales.
- **Registro:** el resultado de cada regla y los registros rechazados se guardan con los nombres del estándar (`aud_resultado_reglas_calidad`, `cuarentena_<capa>_<objeto>`).

### Catálogo preliminar de reglas técnicas (propuesta, a validar)
Son reglas genéricas que no dependen de la definición funcional:

| Tipo | Regla |
|---|---|
| Estructura | El archivo o tabla existe y no está vacío; columnas y tipos esperados; formato de fechas válido. |
| Calidad | Sin nulos en campos llave; sin duplicados sobre la llave; sin valores negativos donde no aplican. |
| Conciliación | Registros de entrada = registros de salida + rechazados; la suma de valores coincide entre Consolidación y la salida. |
| Coherencia | El periodo de los datos corresponde al periodo que se certifica; las fechas caen dentro del rango del periodo. |

Las reglas de **negocio** (variación máxima frente al periodo anterior, rangos por producto, relaciones entre campos actuariales) las define la definición funcional en curso.

## Riesgos del flujo de datos
Ver `Tareas\riesgos-flujo-glue-notificacion.md`. El validador escribe su resultado en el manifiesto de la ejecución, y ese manifiesto es lo que se certifica y transporta (riesgos 1, 2 y 4: versión certificada distinta, totales recalculados y certificación de datos vacíos).

## Lo que se puede adelantar sin acceso
- La especificación del validador: contrato de la regla, resultado, severidad y comportamiento al fallar.
- El catálogo preliminar de reglas técnicas, como insumo para la mesa funcional.
- La plantilla de evidencia de cuadre, que según HU-TR-04 se acuerda con Contabilidad y Actuaría.

## Bloqueos y preguntas
- Definición funcional de las reglas de negocio de reserva matemática.
- Estructura real de los datos de reserva matemática (columnas, llaves, volúmenes): requiere acceso.
- ¿Las validaciones aplican solo a reserva matemática o también a los otros cuatro casos de uso? El diseño las hace reutilizables.
- Umbrales de rechazo tolerables (decisión T-01 del estándar de trazabilidad).
- Si no hay certificación, ¿el flujo se detiene o sigue al paso 4?
