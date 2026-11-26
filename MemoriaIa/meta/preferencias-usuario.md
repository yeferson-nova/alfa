---
tipo: preferencia
estado: vigente
actualizado: 2026-11-25
---
# Preferencias del usuario

- Responder en español y de forma concisa.
- Presentar un plan antes de hacer trabajo pesado y esperar aprobación.
- Las tareas tienen su propia carpeta `Tareas\`, separada de `Docs\`, con contexto corto pero completo para lectura humana.
- Hoy no hay acceso a AWS ni a Git (2026-11-03): avanzar con diseños, especificaciones y análisis sobre lo entregado. *(Actualización 2026-11-05: ya hay Git con push a `main`; Desarrollo solo accede a AWS en DEV, por consola y CloudShell; QA y PRD los despliega y opera otro equipo por release manual.)*
- Todo el stack es AWS ("Atenea" = AWS Athena).
- Todo código nuevo sigue [[est-desarrollo-hexagonal]]: arquitectura hexagonal, código limpio apto para SonarQube, sin valores quemados, pruebas unitarias, idempotencia y logs de error para CloudWatch (2026-11-03).
- El marco normativo sale de los documentos ya entregados (manuales y estándares), no de supuestos. Contexto: estándares bancarios colombianos.
- Analizar siempre tres tipos de brecha: seguridad, costos (Athena) y arquitectura, según los estándares entregados.
- La memoria del proyecto vive en `MemoriaIa\` como notas enlazadas estilo Obsidian. `CLAUDE.md` solo lleva el estándar y el índice, ver [[rutas-workspace]].

- **Alcance (2026-11-05):** nuestra tarea es solo el sistema de jobs de Glue y las lambdas de notificación; lo demás es contexto. Lineamientos en la skill `.claude\skills\niif17-alfa\`.
- **Acceso (2026-11-05, aclarado por el usuario):** Desarrollo solo tiene acceso a **DEV**. QA y PRD se despliegan por un **release manual** de otro equipo. Todo cambio de prod se entrega como paquete (diff mínimo, pruebas, rollback, runbook) y lo ejecuta ese equipo; lo que antes se anotó como "el usuario opera prod" aplica solo a quien trae la evidencia. Los scripts para prod son de solo lectura y los corre el equipo de prod.
- **Despliegue (2026-11-18, aclarado por el usuario; reemplaza la nota de release del 2026-11-05):** DEV, QA y PRD se despliegan igual: el código por PR a la rama del ambiente (`develop`, `quality`, `main`) y Jenkins lo sube; la infraestructura la actualiza el IaC. Nada de código ni infraestructura se sube a mano, ni siquiera en DEV: los scripts de prueba solo comprueban el despliegue, respaldan datos, corren jobs y verifican.
- **Roles (2026-11-05):** nosotros somos Desarrollo y construimos todo el pipeline para entregarlo a Analítica (quien lo recibe y opera). La tarea y el incidente en prod ([[inprod-primas-arl-202606]]) son nuestros: los arreglamos nosotros antes de entregar, no se delegan a Analítica.
- **ETL (2026-11-05):** cuidar especialmente los cálculos de montos y valores, el rendimiento y el costo de cómputo y de consultas (Athena). Detalle en `references\montos-rendimiento-costos.md` de la skill.

## Cómo trabajar con el usuario (aprendido en el incidente de prod, 2026-11-05)
- **Validar con código, no suponer:** cada afirmación del análisis se comprueba con un script o consulta (PASS/FAIL) sobre la evidencia; marcar lo no comprobado como hipótesis. Ver [[inprod-primas-arl-202606]].
- **Explicar en palabras cortas y simples** cuando lo pide; usar ejemplos o analogías, sin jerga.
- **Pasos para la consola web de AWS** (menús, qué mirar, qué concluir) y comandos listos para CloudShell; en DEV el usuario los ejecuta; para prod son de solo lectura y los corre el equipo de prod, que devuelve capturas o archivos. No hay acceso directo desde aquí.
- **Commits sin línea de coautor** y **push directo a `main`** (sin ramas ni PR) cuando lo pide ("sube a git"). Los pedidos de push suelen venir al final de cada tarea.
- **Nombres de archivo fijos**, sin fecha ni hora (ejemplo: `evidencia-primas-arl-202606.zip`). Los `.gitignore` y `.gitattributes` van en la **raíz** del repo, no en subcarpetas.
- Entregables del incidente en `errores\` (md + scripts). **Toda evidencia de AWS (dev y prod) va en `EvidenciasAws\<ambiente>-<tema>\`, fuera de Git** (decisión 2026-11-10). **Excepción (2026-11-25):** las salidas de validación de PCR van en Git, en `Tareas\Modularizacion\PCR\<malla>\validaciones\salidas\`, sin datos personales, IDs de cuenta, IP ni claves ([[proceso-pcr]]); ya no llega por `git pull`.
- El repo pasó a **privado** el 2026-11-09 (antes fue público). Aun así, no subir credenciales ni datos personales innecesarios de prod (correos, ids) y preguntar antes de subir evidencia.
- Se hace `git pull` cuando el usuario dice que subió archivos ("baja la carpeta").
- **Código Python (2026-11-10):** PEP 8, sangría de 4 espacios e identificadores en inglés. Arquitectura modular hexagonal (es nuestra; DevOps solo hace la infraestructura). Entrega en un zip con el nombre del recurso; los commits los hace DevOps. Ver [[est-desarrollo-hexagonal]] y [[est-gobierno-devops-niif]].
- **No cambiar código sin una orden explícita (2026-11-10):** mientras se refina un diseño, solo se hace análisis y documentación. El código se escribe o modifica solo cuando el usuario lo pide de forma explícita ("ejecuta", "hazlo", "implementa"). Preguntas como "¿podemos avanzar?" no son una orden. Antecedente: se aplicó el punto 1.5 (cancelación en Athena) sin orden explícita.

## Agregado el 2026-11-11
- **Configuración por ambiente:** un JSON en S3 con la fuente en Git, desplegado por DevOps. Los argumentos solo llevan valores de cada ejecución y apuntadores. Sin valores por defecto, una sola fuente por valor, y los tags no son configuración. La regla está en la skill: `references\configuracion-por-ambiente.md`.
- **Misma estructura de carpetas en todos los componentes, en inglés:** `domain`, `application`, `adapters/inbound`, `adapters/outbound`, `config`, `test/`. **Un solo `README.md`** por componente: argumentos, claves del JSON, salidas y qué hay en cada módulo.
- **Commits:** título **corto y claro en español**; cuerpo de **máximo 2 líneas**, solo lo que se hizo (sin nombrar fases ni detalles del plan); **siempre sin coautor** (reiterado 2026-11-25). El commit y el push los pide el usuario, normalmente al terminar una tarea ("al terminar haz commit push").
- **Orden explícita:** "aprobado, ejecuta los cambios" cuenta como orden para escribir código. Aprobar una recomendación (por ejemplo, O-2) no da permiso para crear recursos en AWS: lo de Terraform se pide a DevOps.
- **Terminología (2026-11-20):** no decir "extractor". Decir "proceso de extracción estándar" (6 mallas ALFAPRD) y "proceso de extracción de Emisiones" (Acsel-e), en entregables y en la memoria. Los nombres técnicos (`extraccion-estandar-emisiones`, políticas `...-extractor-...`) se escriben tal cual.
- **IaC y DevOps (2026-11-20):** nuestra tarea no es de DevOps. Los repos `iac-*` son de solo lectura para nosotros: los leemos para entender contratos y permisos, y lo que falte se le pregunta o se le pide a DevOps. No suponer de dónde sale un recurso que no está en nuestros repos: puede venir de un repo de DevOps que no tenemos.
- **Sesiones remotas:** todo lo necesario debe quedar en el repo (`MemoriaIa\`, `CLAUDE.md` y la skill), porque la memoria local de Claude no viaja. Al cerrar un bloque de trabajo, actualizar [[traspaso-sesion]].
- **Siglas (2026-11-12):** siempre que se escriba SCD, poner entre paréntesis lo que significa: *(Slowly Changing Dimension, dimensión de cambio lento: guarda el historial de cada registro sin sobrescribirlo)*. Aplica también a otras siglas técnicas poco conocidas si el usuario lo pide.
- **Entregables (2026-11-13):** **nunca publicar como URL** (artifact ni enlace externo); todo entregable es un `.md` local en el repo. Los fixes van en `Tareas\Fixs\<nombre-del-fix>\`, una carpeta por fix, con el informe técnico, las auditorías, las pruebas y demás elementos (ejemplo: `Tareas\Fixs\primasARL\`).
- **Ramas en documentos (2026-11-13):** la rama de trabajo se nombra una sola vez, al inicio (ejemplo: `bugfix/scd-freno-periodo`, creada desde `develop` actualizado, es la de la corrección completa). En el resto del texto no usar frases como "esta rama", "se corrigen en esta rama" o "en esta rama:"; nombrar el archivo o componente (por ejemplo, "en `scd.py`").
- **Ajustes acotados (2026-11-13):** si el usuario pide ajustar algo puntual (por ejemplo, los tiempos de un plan), se ajusta solo eso. No agregar secciones, condiciones, riesgos ni contenido que no estaba en el documento.
- **Deuda técnica con la configuración (2026-11-20, no negociable):** al corregir deuda técnica se revisan y evolucionan también los archivos de configuración de ese componente, en el mismo cambio: los JSON de `cfg-datalake-niif-config-s3`, el registro de DynamoDB `cnf-mallas` y lo que publica la lambda configuradora (que sobrescribe el JSON de S3 y descarta claves que no conoce, como pasó con `umbral_apagado_pct` y el `where` de ARL; ver [[fix-scd-freno-periodo]]). Código y configuración avanzan juntos; un fix que solo toca código no se da por cerrado.
- **Proceso personal ↔ corporativo invisible para Alfa (2026-11-20):** `alfa-repos`, sus scripts (`exportar`, `generar-parches`, `aplicar-parches`, `sincronizar-repos`), `parches/`, `VERSIONES.md` y la existencia de dos Git son solo nuestros. Ningún entregable (`Tareas\`, `Docs\`, repos corporativos, zips) los menciona. En ellos se nombra solo el repo corporativo, su ruta interna y su rama o commit. Regla en `CLAUDE.md`; el proceso se documenta solo en [[repo-alfa-repos]].
- **Redacción de informes y correos a negocio (2026-11-24):**
  - Nombrar siempre el ambiente y la fuente de cada cifra: "Silver en AWS (DEV y QA)", "Oracle ACSELE, tabla `THIRDPARTYMOVEMENTPOLICY`", "Athena DEV". Nunca "el sistema" ni "el origen" a secas.
  - No afirmar lo no comprobado: decir "la tabla X tiene N movimientos que suman Y", no "en el origen está completo".
  - Cada regla o fórmula lleva de dónde sale; si la armamos nosotros, decirlo. Si una cifra no cuadra al centavo, dar la diferencia y decir que está sin explicar.
  - Los hallazgos se describen como fallas, sin "vamos a corregir" ni planes cuando el texto es de hallazgos, y sin repetir ideas.
  - Primera persona singular cuando el usuario lo pide. Decir el código de producto junto al nombre (por ejemplo, "Pymes (55139)").
  - Mensajes al grupo: cortos, humanizados y sin códigos internos de hallazgo.
- **Código sin comentarios (2026-11-17):** el código que escribamos no lleva comentarios ni docstrings; los nombres deben explicar la intención. Aplica a `src/` y a `tests/`.
