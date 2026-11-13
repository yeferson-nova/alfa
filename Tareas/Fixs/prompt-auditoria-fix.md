# Prompt: auditoría técnica de un fix

Copia el bloque y llena los campos entre `< >`.

```text
Necesito una auditoría técnica del fix <NOMBRE_DEL_FIX> en la malla <MALLA> (NIIF 17, Seguros Alfa).
- Síntoma o incidente: <QUÉ PASÓ, CUÁNDO, EN QUÉ AMBIENTE>
- Repo: <REPO>   Rama: <RAMA> (creada desde <develop|quality> actualizado)
- Archivos a auditar: <ARCHIVO PRINCIPAL> y <OTROS ARCHIVOS DEL MISMO REPO>
- Evidencia disponible: <logs, parámetros, consultas de Athena, capturas; o "ninguna">
- Carpeta del fix: Tareas\Fixs\<carpeta>\

No asumas nada: todo se comprueba con el código, la arquitectura o una simulación. Lo que no se pueda comprobar va marcado "por confirmar". No modifiques código de los repos, no hagas commit ni push y no publiques URLs.

## Proceso
0. Contexto: lee CLAUDE.md, MemoriaIa\00-INDEX.md, traspaso-sesion, preferencias-usuario y las notas de la malla o del incidente. Carga la skill niif17-alfa.
1. Código: ubica los archivos en alfa-repos (foto de la rama según VERSIONES.md) y confirma repo, rama y commit. Si hay copia de lo desplegado (QA o PRD), haz el diff ignorando finales de línea.
2. Flujo: lee la Step Function de la malla (EvidenciasAws) y arma el mapa etapa → job → repo → archivo, con argumentos, reintentos, Catch y timeouts. Revisa el contrato entre jobs (parametros_extraccion.json), la config de la malla (cfg-datalake-niif-config-s3) y el configurador.
3. Auditoría: separa el problema raíz de los demás hallazgos. Cada hallazgo lleva ID, severidad (Crítico, Alto, Medio o Bajo), qué pasa, a qué componente afecta, evidencia (archivo:líneas, QA o simulación) y una corrección sin sobreingeniería que no pierda lo que ya funciona. Mira cada archivo como parte del flujo: cómo afecta a las etapas anteriores y siguientes.
4. Validación con código: ejecuta los scripts reales, sin modificarlos, con PySpark local (entorno con uv: Python 3.11 + pyspark 3.5.5) y Glue, S3 y Oracle simulados en memoria.
   - Casos del archivo principal.
   - Cadena completa de jobs como la corre la Step Function, con escenarios: control (el fix funciona), antes del fix (reproduce el síntoma), problema raíz y uno por hallazgo grave.
   - Si la simulación contradice una hipótesis, corrige el hallazgo y dilo.
5. Estructura del código: mide líneas, lógica fuera de funciones, anidamiento, función más larga, imports dentro de funciones, funciones copiadas entre jobs, pruebas existentes y configuración de Sonar y tox. Los hallazgos van como COD-xx.
6. Informe: escribe Tareas\Fixs\<carpeta>\informe-tecnico.md. Los scripts de medición y simulación van en Tareas\Fixs\<carpeta>\pruebas\.
7. Memoria: actualiza pendientes, rutas-workspace y la nota del incidente o de la malla. Corre el validador de la memoria (alfa-repos\scripts\validar-memoria.py) y busca caracteres de control en lo que edites.

## Estructura del informe
- Título, Para (equipo de desarrollo, BA y líder técnico), Fecha y Alcance.
- Resumen de máximo 10 líneas.
- Índice enlazado con anclas explícitas (<a id="..."></a>).
- Repo que se toca: tabla repo, archivo y qué se hace. La rama se nombra una sola vez, aquí; después se habla del archivo o componente, nunca de "esta rama".
- 1. Descripción general, con el flujo de etapas.
- 2. Cómo se validó y 2.1 Resultados de la simulación (tabla: escenario, qué se hizo, resultado, qué confirma).
- 3. Problema raíz, con flujos en texto: antes del cambio, con el cambio, lo que sigue abierto y la corrección (tabla de IDs).
- 4. Otros problemas encontrados: 4.1 archivo principal, 4.2 otros archivos del repo, 4.3 estructura del código (tabla de métricas y COD-xx), 4.4 otros componentes, con responsable. Cada tabla de código cierra con una línea "Se conserva".
- 5. Orden de trabajo: primero lo corto que protege datos y no depende de nadie; luego lo que espera a BA; después lo que necesita DevOps; al final lo menor.
- 6. Preguntas para BA, indicando qué hallazgo desbloquea cada una.

## Reglas de redacción
- Español, lenguaje neutro y humano, frases cortas, sin jerga innecesaria y sin sobredimensionar.
- Cada vez que aparezca SCD, va con su significado entre paréntesis: SCD (dimensión de cambio lento).
- No mencionar en el informe los scripts usados para medir o validar.
- Nada de datos sensibles de producción (correos, cuentas, identificadores de personas).
- Solo .md local; nunca publicar como URL.
```
