---
tipo: hu
estado: vigente
actualizado: 2026-11-03
---
# HUs Tema 04: Seguridad y Gobierno de Acceso al Dato (HU-SG-01 a 24)

Fuente: `Docs\03-historias-usuario\04_HUs_Tecnicas_Seguridad_Gobierno_Acceso_Datos_v1.xlsx` (v1.0, sep-2026). Estándar obligatorio con backlog cerrado. "El permiso se define una vez en AWS y se despliega por código."

## Reglas clave
- **Identidad (HU-SG-01, 02 y 04):** IAM Identity Center federado con el directorio corporativo; grupos corporativos por convención; administrador del lago como rol asumible con aprobación; segregación entre quien solicita, aprueba y administra.
- **Permisos como código (HU-SG-03):** repositorio único de permisos; toda concesión referencia su solicitud aprobada.
- **Lake Formation (HU-SG-05 a 09):** se retira el permiso heredado por defecto de toda base y tabla; etiquetas LF-Tags; nada se publica sin clasificar.
- **Las cuatro rutas de consumo:** visualización, interfaz de servicio, consulta directa y ad hoc. Las cuatro resuelven el permiso en el mismo punto y **el permiso efectivo es idéntico por cualquier ruta**.
- **Athena (HU-SG-11):** grupos de trabajo por área y ambiente con identidad habilitada.
  - **Topes de escaneo por consulta y de gasto por período**, con aviso al líder antes del corte. El tope de escaneo es **control de costo y de extracción** a la vez.
  - Resultados cifrados, en una ubicación propia con ciclo de vida corto y sin acceso de terceros.
- **Protección del dato:** seudonimización en Plata con sal en Secrets Manager (HU-SG-15); **llaves KMS por dominio con rotación y separación de funciones** (HU-SG-16); detección automática de datos personales con Macie (HU-SG-17).
- **Ciclo de vida del acceso:** solicitud y aprobación; recertificación periódica; excepciones con vigencia obligatoria (HU-SG-18 a 20).
- **Identidades no humanas (HU-SG-21):** cada rol de proceso o servicio tiene perfil, dueño, alcance, vigencia y recertificación.
- **Evidencia:** registro de acceso consolidado con retención por clasificación (HU-SG-22); **detección de deriva de configuración** (HU-SG-23); acceso anómalo con umbrales y pruebas negativas (HU-SG-24).

## Hallazgos de Arquitectura A-01 a A-15 (contexto)
- **A-03:** las 63 tablas del lago conservan el permiso genérico.
- **A-04:** la clasificación vive en Excel.
- **A-07:** cifrado sin segregación de llaves.
- **A-08:** alertas sin umbral, destinatario ni plazo.
- **A-11:** registro de acceso no consolidado.
- **A-12:** propagación de clasificación sin regla.
- **A-13:** umbral de agregación de P3 no definido.
- **A-14:** ambientes no productivos sin método.
- **A-15:** deriva de configuración por cambios en consola.

## Relevancia ([[tareas-aws]])
- **Costos de Athena:** HU-SG-11 es la base normativa para la matriz COS de la fase 3. Hay que verificar si los workgroups actuales tienen topes y ciclo de vida de resultados.
- **Lambdas del paso 3:** son identidades no humanas, con rol propio de mínimo privilegio, dueño y recertificación (respalda [[brechas-seguridad]] #8).
- **URL prefirmada del CSV:** debe respetar KMS por dominio y no saltarse Lake Formation (riesgo A-01: el permiso vive fuera del catálogo).
- **Enlaces Certificar / No certificar:** HU-SG-04 y HU-SG-18 exigen identidad federada y trazabilidad de la decisión.
