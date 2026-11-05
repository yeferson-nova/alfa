# Roadmap del proyecto NIIF 17: cerrar el incidente, entregar las tareas, auditar y migrar al estándar

**Estado:** propuesta del 2026-11-05, para validar con el equipo.
**Quién lo hace:** Desarrollo. Nosotros construimos todo el pipeline y se lo entregamos a Analítica.
**Cómo leerlo:** son cuatro frentes que avanzan al mismo tiempo. Cada paso tiene su tiempo en días hábiles y lo que debe quedar listo al terminar.

## Antes de empezar: lo que tenemos que tener claro

- **Solo tenemos DEV.** QA y producción las despliega otro equipo con un release manual. Por eso, lo de producción lo hacemos como paquete de cambio: lo probamos en DEV, se lo entregamos a DevOps, ellos lo suben a QA y ahí lo probamos. Para prod les pedimos que ejecuten.
- **Todavía no hay un flujo de Git autorizado** para desplegar. Mientras llega, el artefacto viaja directo a DevOps.
- **Desconocemos varias cosas** (el código del manager, el de la cadena final, los JSON de configuración, cuánto cuesta y tarda una corrida real). Por eso a todos los tiempos les sumamos **30 %**. Cuando cerremos la auditoría revisamos si ese colchón se queda, sube o baja.
- **Supuesto de equipo:** unas tres personas trabajando en paralelo. Si somos menos, los tiempos se estiran.
- Lo que depende de otros (DevOps, el equipo de release, negocio, los trámites de SES) puede mover las fechas aunque nosotros cumplamos.

---

## Frente 1. Cerrar el incidente de producción (temporal)

Lo que pasó: la certificación de Primas ARL 202606 salió vacía porque la extracción trajo una parte y el SCD apagó todo lo demás. Gold de producción quedó con 209 registros activos. Aquí buscamos tres cosas: que no se repita, que se recupere el dato y que quede evidencia. El arreglo de fondo viene con la reestructuración.

| Paso | Qué hacemos | Tiempo | Queda listo cuando |
|---|---|---|---|
| I1 | Pedimos al equipo de release que desactive las programaciones que repiten un periodo ya procesado, que active el versionado de Gold y que nadie certifique 202606 | 1,5 días | La solicitud está enviada y confirmada |
| I2 | Freno en el SCD: si va a apagar más de un porcentaje de Gold, el job falla y no escribe. Ya hay un borrador; decidimos si lo conservamos | 2,5 días | Probado en DEV: la misma corrida falla controlada y Gold queda igual |
| I3 | Alinear la extracción de prod con la de DEV, que conserva los lotes de respaldo. Primero le pedimos a prod que compare los scripts, porque esa diferencia todavía es hipótesis | 4 días | Sabemos si prod tiene otra versión y, si la tiene, queda el cambio probado en DEV |
| I4 | Que el correo no salga si el resumen o los totales vienen vacíos, y completar la consulta `summary` que falta en el JSON de prod | 2,5 días | Probado en DEV con un caso sin datos |
| I5 | Entregamos los paquetes a DevOps y los probamos en QA | 6,5 días | Resultado de QA documentado |
| I6 | Pedimos la recuperación de 202606: restaurar Gold desde una versión anterior al 2 de noviembre. Solo si no hay versión, respaldar, limpiar los 5 segmentos y re-ejecutar una sola vez con los candados ya puestos | 4 días | Gold de prod con los activos de 202606 y un conteo que lo demuestre |
| I7 | Revisar si las otras 5 mallas tienen el mismo riesgo y entender por qué el 91 % de Gold tiene `per_proceso_point` nulo | 5 días | Informe con hallazgos y su ID |

**Orden que respetamos:** primero que prod desactive programaciones y versione Gold (I1), luego desplegamos los candados (I2 a I5) y solo al final se recupera (I6). Si se recupera antes de poner el freno, el fallo se repite.
**Tiempo del frente:** unas 3 semanas y media. I7 corre en paralelo.

---

## Frente 2. Entrega de las tareas (carpeta Tareas)

Estas son las tareas que nos asignaron. Algo ya está hecho en local y falta desplegarlo y probarlo en QA.

| Paso | Tarea | Dónde estamos | Tiempo | Queda listo cuando |
|---|---|---|---|---|
| E8 | Pedir los trámites de SES: verificar el dominio (DNS), salir del *sandbox* y el visto bueno de seguridad | Sin pedir. Tiene plazo largo y no depende de nosotros | 1,5 días de gestión | Trámites abiertos con responsable |
| E1 | **T2-A:** un solo código en las 6 lambdas de configuración, sin cambiar nombres ni programaciones | Lambda general hecha (174 pruebas), sin desplegar. Falta confirmar el catálogo de 5 mallas, probar que genera los mismos 4 JSON y definir tabla de idempotencia y rol por IaC | 10,5 días | Las 6 funciones corren el mismo artefacto y generan lo mismo que antes |
| E2 | **T2-B:** una sola lambda y el manager enviando la malla en el evento | Falta el código del manager y dejar un solo programador | 10,5 días | Las programaciones apuntan a una función y cada malla se ejecuta igual |
| E3 | **T1a:** correo por SES en vez de SNS | Job reescrito (451 pruebas), sin desplegar. Falta identidad de SES, rol y subir artefactos | 5 días | Correo de los casos llega por SES en QA, con los rebotes registrados |
| E4 | **T1b:** correo en HTML | Las plantillas nuevas existen. Falta conectarlas al job | 6,5 días | Correos en HTML con el mismo contenido y enlaces |
| E5 | **T1c:** que soporte se entere de cualquier fallo | Falta decidir CloudWatch contra Grafana | 10,5 días | Un fallo simulado llega a soporte con su `ejecucion_id` |
| E6 | **T1d:** escenarios de notificación (éxito, fallo, diferencias, sin datos, timeout, no certificar, enlace vencido) | Sin empezar | 5 días | Catálogo aprobado y probado |
| E7 | **T3:** validaciones de consistencia de reserva matemática | Bloqueado por la definición funcional del negocio. Sí podemos adelantar el esqueleto de la compuerta | 19,5 días una vez llegue la definición | Una regla bloqueante detiene el flujo y aparece en el correo |

**Orden:** primero E8 y E1, luego E2. E3 y E4 van en paralelo porque son otro componente. E5 y E6 cuando haya logs comunes. E7 apenas llegue la definición; hay que pedirla ya.

---

## Frente 3. Auditoría de lo que tenemos

Antes de migrar necesitamos saber exactamente qué hay. Hoy parte del código no lo tenemos y varias conclusiones son hipótesis.

| Paso | Qué hacemos | Tiempo | Queda listo cuando |
|---|---|---|---|
| A1 | Completar el inventario: código del manager, trigger de notificación, copy, move y SFTP, y los JSON de configuración | 4 días | Todo ese código está en el repositorio |
| A2 | Correr el script de comparación en DEV y que prod lo corra en PRD (es solo lectura) | 2,5 días | Dos resúmenes con lo que existe, lo que falta y lo que tiene riesgo |
| A3 | Comparar contra el diagrama general y actualizar el diagrama con lo que de verdad existe | 4 días | Diagrama al día con una sola fuente de verdad |
| A4 | Juntar todos los hallazgos (PP, RYC, FLU, LMB, SF, INC) en una matriz con IDs de seguridad, costos, arquitectura y cumplimiento | 6,5 días | Matriz completa |
| A5 | Medir una ejecución real: tiempo por etapa, costo de Glue y bytes que escanea Athena | 4 días | Línea base de tiempo y costo |
| A6 | Línea base de SonarQube del código actual | 4 días | Reporte inicial |
| A7 | Pruebas que describen cómo se comporta hoy cada job, para poder refactorizar sin romper | 13 días | Pruebas verdes sobre el código actual |
| A8 | Decidir qué hacemos con la evidencia de prod que ya está en el repositorio público (correos, IDs de Cognito) | 2,5 días | Decisión tomada y ejecutada |

**Importante:** A1, A2 y A4 alimentan la migración. A8 va primero porque es un riesgo de seguridad abierto.

---

## Frente 4. Migración al estándar de desarrollo

Aquí dejamos todo el código ordenado: repositorios con nombre estándar, el mismo artefacto para DEV, QA y PRD, sin valores quemados, con pruebas y listo para que Analítica lo opere.

| Paso | Qué hacemos | Tiempo | Queda listo cuando |
|---|---|---|---|
| M1 | Decidir la estructura de repositorios (seguimos con la propuesta A de forma provisional) y enviar las 18 preguntas a DevOps | 2,5 días | Decisión escrita y correo enviado |
| M2 | Plantilla única de repositorio: arquitectura hexagonal, cobertura de al menos 80 % y un solo Python. La plantilla de DevOps hoy tiene cobertura en 0 % | 4 días | Plantilla lista para usar |
| M3 | Código neutro entre ambientes: un archivo de configuración por ambiente y una prueba que falle si encuentra `-dev-`, `-qa-`, `-prd-`, cuentas o ARN en el código | 4 días | Prueba en verde |
| M4 | Librería `comun` (lecturas de JSON y S3, errores, logs) y el contrato entre etapas con un manifiesto por ejecución | 10,5 días | Paquete con pruebas y esquema versionado |
| M5-1 | **Oleada 1:** lo que ya está hecho (job de notificación y lambda general) en sus repositorios | 4 días | Artefacto desplegado en DEV |
| M5-2 | **Oleada 2:** extracción y SCD, con el freno como compuerta y modo de carga explícito | 15,5 días | Pruebas de más de 80 % y mismo resultado que el job actual |
| M5-3 | **Oleada 3:** validación técnica bloqueante, remediación y certificación (filas reales, montos en DECIMAL, Athena con tope) | 15,5 días | El cuadre entra = sale + cuarentena queda registrado |
| M5-4 | **Oleada 4:** los 6 Silver, de menor a mayor riesgo: Endosos, Auxilios, Rentas, ARL, y al final Reserva ARP y RVI | 23,5 días | Cada Silver da el mismo resultado que el SQL actual |
| M5-5 | **Oleada 5:** la cadena final (trigger, copy, move y SFTP) con reintentos limitados e idempotencia | 10,5 días | Sin reintentos infinitos ni registros que bloqueen |
| M6 | Una sola plantilla de Step Function para las 6 mallas y limpiar crawlers y catálogo | 10,5 días | Las 6 máquinas salen de una plantilla |
| M7 | Todo lo creado a mano pasa a Terraform | 13 días | Sin recursos manuales |
| M8 | Promover DEV → QA → PRD con rollback probado, prueba de punta a punta, SonarQube y entrega a Analítica | 13 días | Acta de entrega |

---

## Cómo se ven los tiempos en el calendario

Con unas tres personas y los frentes en paralelo, el camino largo sale **de unas 17 semanas (alrededor de cuatro meses)**. Sin el colchón del 30 % serían unas 13.

| Cuándo | Qué esperamos tener |
|---|---|
| **Semanas 1 y 2** | Pedidos al equipo de release (I1), borradores de I2 a I4, trámites de SES, decisión sobre la evidencia pública, inventario y script de comparación, decisión de estructura y plantilla |
| **Semanas 3 y 4** | Paquetes del incidente en QA (I5), recuperación pedida (I6), auditoría con el diagrama y la matriz, código neutro y `comun` en marcha |
| **Hito 1, semana 4** | Incidente contenido, estructura decidida y la auditoría base hecha |
| **Semanas 5 a 8** | Oleadas 1 y 2 (extracción y SCD), T2-A y T1a en QA, línea base de costo y de SonarQube |
| **Hito 2, semana 8** | Motores genéricos en el repo estándar y probados |
| **Semanas 9 a 12** | Oleadas 3 y 4 (calidad y los 6 Silver), T2-B, T1b, T1c y T1d. T3 entra cuando llegue la definición |
| **Hito 3, semana 12** | Calidad y los 6 Silver migrados |
| **Semanas 13 a 17** | Cadena final, plantilla de Step Function, Terraform, promoción y entrega |
| **Hito 4, semana 17** | Entrega a Analítica |

**Lo que más puede mover estas fechas:**
1. La respuesta de DevOps sobre repositorios y despliegue.
2. Lo que tarde el equipo de release en ejecutar lo que le pedimos.
3. La definición de negocio para T3.
4. Los trámites de SES.
5. Lo que salga de la auditoría. Si aparece mucho código que no teníamos, el colchón se queda corto.

## Cuándo damos la solución por normalizada

Cada pieza vive en su repositorio con nombre estándar. El mismo artefacto se despliega en DEV, QA y PRD sin tocar el código. No hay valores quemados, las pruebas cubren más del 80 %, hay un manifiesto por ejecución, las alarmas están activas y no queda ningún recurso creado a mano.
