# Limpieza de DEV: qué recursos se usan y cuáles sobran

Fecha de la evidencia: 2026-11-10. Script de solo lectura: `Backend\cloudshell-inventario\evidencia-uso-dev.sh`. La salida está en `EvidenciasAws\dev-uso-recursos\` y no se versiona porque trae ARN con el número de cuenta. Ventana: últimos 90 días.

**Este documento no autoriza borrar nada.** Cada recurso se retira solo cuando se confirma quién es su dueño. Lo que se creó por Terraform (etiqueta `terraform-aws-modules` o `ManagedBy`) se retira por Terraform, no desde la consola.

## 1. Alcance
- **Nuestro:** la cadena de notificación y certificación (paso 3), las 6 mallas (Primas Rentas, Endosos, ARL, Auxilios Funerarios, Reserva Matemática ARP y RVI) y el gestor de configuración (T2).
- **No es nuestro, no se toca:**
  - Plataforma y seguridad: CloudGuard, Control Tower, Quick Setup y `ALFA-DEV-DATALAKE-PipelineObserver` (este último es CloudFormation y lo usamos como base de monitoreo).
  - Clientes y SMB: `datalake-dev-*smb*`, `*clientes*`, `contingency` y `rollback-bronce` de Terraform.
  - Otras mallas: `niif-dev-accountingtool`, PCR Vida, Generales, Satélites, Previsional, Emisiones y gestor de archivos. Hay corridas de Glue de hoy (una en `RUNNING`), así que alguien está trabajando ahí.
  - `EnvioSftpFis`, que recibe notificaciones de S3 del bucket de landing de Prophet.

## 2. Vigente y alineado (se queda)
| Recurso | Evidencia (90 días) |
|---|---|
| `niif-dev-manager-configuration` | Stream de `niif-dev-prg-programaciones` en `Enabled`; 74 invocaciones y 7 errores |
| `niif-dev-configuration-<malla>` (6) | Entre 8 y 30 invocaciones cada una; Terraform |
| `niif-dev-composite-notification-evaluator` | Stream de `composite-notification-status`; 213 invocaciones; lo llaman 11 Step Functions |
| `niif-dev-notification-certification-trigger`, `-copy-s3-cert`, `-move-s3-certification` | Stream de `niif-dev-notification-certification` en `Enabled` (94, 18 y 1 invocaciones) |
| `niif-dev-certification` (lambda) | API `datalake-dev-niif-api-gtw`: `POST /certify` y `POST /not_certify`; 45 invocaciones |
| `niif-dev-short-link-redirect` | Mismo API: `GET /r` y `GET /redirect`; 164 invocaciones |
| `niif-post-login` | Mismo API: `GET /post_login`; 13 invocaciones. **No tiene etiquetas de Terraform** |
| `datalake-dev-niif-send-via-sftp` | Notificación de S3 de `niif-dev-transport`; **0 invocaciones** (ver §5) |
| Glue `niif-dev-certification` / `niif-dev-notification-certification` | 92 y 100 corridas |
| Glue de las 6 mallas (extracción, val-tecnica, remediación, silver ×6, scd) | Los llaman las Step Functions `niif-dev-<malla>` |

**ARQ-28 queda parcialmente resuelta:** el API `datalake-dev-niif-api-gtw` tiene el WAF `datalake-dev-niif-apigateway-waf` en la etapa `dev`.

## 3. Nuevos y sin documentar (preguntar al equipo antes de clasificar)
| Recurso | Evidencia |
|---|---|
| `niif-dev-configuration-general` | Cambiada el 2026-11-07. 82 invocaciones y **27 errores**. Es un configurador general en modo validación (`publicar=False ejecutar=False`) que ya cubre ARP, RVI y Satélites (prepara PCR Vida). Su log es el mismo de los `configuration-<malla>` desplegados, así que viene del código actual y **no es nuestro T2 hexagonal**. Se la invocó en lote el 2026-11-07 a las 05:51. ¿Quién la mantiene? ¿Reemplaza a las 6 `configuration-<malla>`? Choca con T2 |
| `niif-dev-configurador-mallas` | Cambiada el 2026-11-05 con etiquetas de Terraform; 0 invocaciones. ¿Es el despliegue de T2 (`sc-niif-configurador-mallas`)? |
| `niif-dev-configurador-pcr-vida` | Cambiada el 2026-11-03; 9 invocaciones. Malla PCR Vida (no es nuestra) |

## 4. Candidatos a retirar (dentro de nuestro alcance)
| Grupo | Recursos | Evidencia | Antes de retirar |
|---|---|---|---|
| **API vieja** | API `Certificación API` (`53x06brwqh`, **sin WAF**), `postLogin`, `datalake-dev-niif-short-link-redirect`, `datalake-dev-niif-certification` | `/r` y `/certificar` con 0 invocaciones; `postLogin` con 46, **la última el 2026-09-25**. El cliente de Cognito `datalake-dev-niif-api` todavía tiene como callback `https://53x06brwqh…/dev/post_login`, junto al del API nuevo | Primero quitar ese callback del cliente de Cognito (por Terraform o DevOps) y después retirar el API y sus 3 lambdas. Brecha **SEG-14** |
| **G1** | `datalake-dev-niif-notification-certification`, `-copy-s3`, `-move-s3` | 0 invocaciones; mappings en `Disabled` contra la tabla `datalake-dev-notification-certification`, que ya no aparece | Ninguna dependencia vista |
| **G3** | `niif-dev-programador-configuraciones`, `niif-dev-configurador-<malla>` (6), schedule `niif-dev-prueba-auxilios-*` (`DISABLED`) | Los dos mappings del programador están en `Disabled` (**ya no hay doble ejecución**). Última invocación: programador el 2026-11-03; configuradores ARL y ARP el 2026-10-22 | Saber quién apagó los mappings y si alguien los usa a mano |
| **G2** | `niif-dev-programador-cargues`, `-configurador-extraccion`, `-iniciador-ejecucion`, `-finalizador-ejecucion`, `-programacion-cargues-target`, regla `niif-dev-finalizar-ejecucion-primas-arl` | La cadena de programación está quieta: el programador corrió por última vez el 2026-09-15 y el iniciador el 2026-09-18, aunque el stream sigue en `Enabled`. **El finalizador sigue vivo:** la regla `niif-dev-finalizar-ejecucion-primas-arl` lo dispara cuando termina la Step Function de ARL (última vez el 2026-11-06, la misma corrida de ARL) y falla 39 de 53 veces | **Verificado:** el finalizador falla con `KeyError: 'idAutomatizacion'` en la línea 238, antes de escribir en `niif-dev-gestor-configuracion` (la Step Function vigente ya no le pasa ese campo). Quitar la regla no cambia ningún dato. Lo demás de G2: confirmar el dueño |
| **SF y jobs viejos** | `orquestador-niif17-*` (10 Step Functions, nunca ejecutadas), `datalake-dev-niif-{val-tecnica,scd,remediacion,extraccion-estandar}`, `job_prueba_*`, `niif-dev-notification-certification-ses-test`, `JOB_ESTANDARIZACION_*` | 0 corridas en 90 días, o solo en septiembre | Confirmar el dueño |
| **Pruebas sueltas** | `certification`, `CheckValidation`, `ReProcessLambda`, `rollback-bronce` (zip), `test`, `test-lambda`, `testMapFunction`, `testNotifications`, `PRUEBA_JQ`, `Prueba_MC`, `Prueba_MC_1`, `deleteModel`, `files-struct-quality` | 0 invocaciones | Confirmar el dueño: no se sabe si son del equipo |

## 5. Hallazgos nuevos
- **SEG-14:** el API viejo `Certificación API` está publicado sin WAF, sigue recibiendo tráfico en `/post_login` y tiene rutas `POST /certificar` y `/no_certificar` que todavía llevan a una lambda.
- **ARQ-30 (confirmada, el transporte a FIS no funcionó nunca en DEV):**
  - Del 2026-08-19 al 2026-09-25, **todos** los Certificar fallaron en `copy-s3-cert` con `The table does not have the specified index: certCode` (RVI, ARP, Auxilios, Rentas y Endosos). La lambda tragó el error (ARQ-26): los registros se quedaron en `CERTIFICATE` sin alarma.
  - El 2026-10-09 copy sí copió dos archivos de ARL a `s3://niif-dev-transport/export/primasArl/`, pero hoy `export/` está vacío y `datalake-dev-niif-send-via-sftp` no tiene ningún log. El SFTP no se disparó y alguien o algo borró los archivos (hipótesis: una regla de ciclo de vida, o la notificación de S3 se configuró después del 2026-10-09).
  - Consecuencia: **el paso Certificar → FIS no se ha probado de punta a punta en DEV.** Es un bloqueante para dar la cadena por lista.
- **Errores altos en piezas vigentes:** `configuration-general` 27 de 82, `trigger` 15 de 94, `manager` 7 de 74.

## 6. Siguiente evidencia (solo lectura)
1. Fecha de la última invocación de G2, G3 y de `postLogin` (último evento en sus grupos de logs).
2. URL de callback de los clientes de Cognito, para saber si alguna apunta al API viejo.
3. Filtro de la notificación de S3 de `niif-dev-transport` comparado con la ruta donde escribe `copy-s3-cert`.
4. Quién invoca `configuration-general` (logs o CloudTrail).

Resultados de 1 a 3 (2026-11-10): ya incorporados en §4 y §5. Siguen pendientes el 4 y la verificación de ARQ-30.
