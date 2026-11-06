# Patrones de diseño para datos de seguros (NIIF 17)

Patrones de industria aplicados al contexto de Alfa. Los que ya están en un documento del banco se citan; los demás son **recomendación del equipo** y se marcan como propuesta al registrarlos.

## Cifras regulatorias
- **Cifra demostrable:** toda cifra publicada se reconstruye desde la fuente: fuente → lote → regla → capa → consulta → correo → transporte (CBCF, HU-TR-04). Si no se puede reconstruir, no se certifica.
- **Cuadre entre capas (reconciliación):** cada transición cumple `entra = sale + cuarentena`; una diferencia bloquea (HU-TR-04, ETL 6.2). Además, cuadre de **valores** (sumas de primas, reservas) y no solo de conteos.
- **Aritmética decimal:** importes en DECIMAL con escala definida, redondeo explícito y en un solo lugar. Nunca `float` para dinero ni para reservas. Los totales se calculan una vez y se transportan, no se recalculan en otra herramienta.
- **Cifras de control:** comparar totales calculados contra los de la fuente o del periodo anterior con umbral; variaciones fuera de rango van al correo como alerta (precedente: aprobación con cifras de control de Nuevos Cargues, `[[est-nuevos-cargues]]`).

## Periodos y cierres
- **Periodo de negocio explícito** (`AAAAMM`) en la entrada, en las rutas y en cada registro. Nunca deducido de la fecha del sistema.
- **Cierre inmutable:** un periodo certificado no se sobrescribe. Un ajuste genera una nueva versión del periodo, con su propia certificación y trazabilidad a la anterior.
- **Reproceso idempotente por periodo:** reprocesar `202606` produce el mismo resultado y no toca otros periodos. Borrar el rango y recargar, nunca acumular (DataStage 8.3, aplicable por analogía).
- **Separar corrida de periodo:** una corrida (ejecución) puede traer datos de varios periodos; los controles se hacen por periodo, la trazabilidad por corrida.

## Historia y versiones
- **SCD tipo 2** para Gold: vigencia desde/hasta, registro activo, lote de origen. El cierre por ausencia solo vale con foto completa (ver lineamientos Glue).
- **Bitemporalidad (propuesta):** distinguir fecha de negocio (a qué periodo pertenece) y fecha de registro (cuándo lo supo el sistema). Permite responder "qué se certificó en su momento" frente a "qué se sabe hoy", que es lo que pide una auditoría.
- **Datos inmutables por lote:** Bronce y Landing no se modifican; se agregan lotes. La corrección ocurre aguas abajo y deja traza.

## Aprobaciones
- **Cuatro ojos (maker-checker):** quien produce la cifra no la aprueba; el número de aprobadores sale de metadata. Segregación entre quien solicita, aprueba y administra (HU-SG-04).
- **Aprobación asociada a una versión:** se aprueba un manifiesto con checksum; si los datos cambian después, la aprobación ya no vale.
- **Decisión auditada:** quién, cuándo, desde dónde, qué versión, con retención de 7 años.
- **Espera explícita:** el flujo queda en estado "esperando certificación" con plazo; vencido el plazo, escala (no se queda abierto sin aviso).

## Calidad y gobierno
- **Metadata como contrato:** la ingesta se declara, no se programa (HU-GI). Reglas de calidad, consultas de certificación y destinatarios versionados y aprobados por PR.
- **Compuerta + cuarentena:** validar antes de escribir, apartar lo rechazado con traza, detener si la severidad lo exige (HU-TR-02/03).
- **Taxonomía de errores única** y catálogo cerrado de eventos (HU-GI).
- **Linaje automático** a nivel de columna (HU-LN).

## Integración y eventos
- **Manifiesto / claim check:** las etapas se pasan una referencia inmutable a los datos (ruta + checksum), no los datos ni un archivo compartido mutable.
- **Outbox / estado antes del efecto:** registrar la intención en DynamoDB antes de enviar correo o transportar, y marcar el efecto al terminar; así un reintento sabe qué falta.
- **Consumidores idempotentes** para streams y eventos "al menos una vez"; cola de fallos con alarma.
- **Orquestación sobre coreografía** en el camino crítico de la cifra (Step Functions con espera de tarea), eventos para lo accesorio (métricas, avisos).

## Antipatrones vistos en este proyecto (no repetir)
Copias de código por malla con *drift*; valores por defecto de otra malla u otro ambiente; validación que nunca detiene; archivo de estado fijo compartido; SCD que apaga por ausencia en carga incremental; CSV con encabezado tomado como "con datos"; correo enviado con totales vacíos; streams con reintentos infinitos sin destino de fallo; timeouts de 48 h; Athena sin workgroup; dos programadores sobre el mismo stream.
