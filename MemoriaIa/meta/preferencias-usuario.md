---
tipo: preferencia
actualizado: 2026-11-11
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
- **Release (2026-11-05):** el artefacto (script o paquete) se entrega a DevOps, que lo sube a QA; ahí se prueba. Todavía no hay flujo Git autorizado para el despliegue. QA y PRD siguen por release manual.
- **Roles (2026-11-05):** nosotros somos Desarrollo y construimos todo el pipeline para entregarlo a Analítica (quien lo recibe y opera). La tarea y el incidente en prod ([[inprod-primas-arl-202606]]) son nuestros: los arreglamos nosotros antes de entregar, no se delegan a Analítica.
- **ETL (2026-11-05):** cuidar especialmente los cálculos de montos y valores, el rendimiento y el costo de cómputo y de consultas (Athena). Detalle en `references\montos-rendimiento-costos.md` de la skill.

## Cómo trabajar con el usuario (aprendido en el incidente de prod, 2026-11-05)
- **Validar con código, no suponer:** cada afirmación del análisis se comprueba con un script o consulta (PASS/FAIL) sobre la evidencia; marcar lo no comprobado como hipótesis. Ver [[inprod-primas-arl-202606]].
- **Explicar en palabras cortas y simples** cuando lo pide; usar ejemplos o analogías, sin jerga.
- **Pasos para la consola web de AWS** (menús, qué mirar, qué concluir) y comandos listos para CloudShell; en DEV el usuario los ejecuta; para prod son de solo lectura y los corre el equipo de prod, que devuelve capturas o archivos. No hay acceso directo desde aquí.
- **Commits sin línea de coautor** y **push directo a `main`** (sin ramas ni PR) cuando lo pide ("sube a git"). Los pedidos de push suelen venir al final de cada tarea.
- **Nombres de archivo fijos**, sin fecha ni hora (ejemplo: `evidencia-primas-arl-202606.zip`). Los `.gitignore` y `.gitattributes` van en la **raíz** del repo, no en subcarpetas.
- Entregables del incidente en `errores\` (md + scripts). **Toda evidencia de AWS (dev y prod) va en `EvidenciasAws\<ambiente>-<tema>\`, fuera de Git** (decisión 2026-11-10); ya no llega por `git pull`.
- El repo pasó a **privado** el 2026-11-09 (antes fue público). Aun así, no subir credenciales ni datos personales innecesarios de prod (correos, ids) y preguntar antes de subir evidencia.
- Se hace `git pull` cuando el usuario dice que subió archivos ("baja la carpeta").
- **Código Python (2026-11-10):** PEP 8, sangría de 4 espacios e identificadores en inglés. Arquitectura modular hexagonal (es nuestra; DevOps solo hace la infraestructura). Entrega en un zip con el nombre del recurso; los commits los hace DevOps. Ver [[est-desarrollo-hexagonal]] y [[est-gobierno-devops-niif]].
- **No cambiar código sin una orden explícita (2026-11-10):** mientras se refina un diseño, solo se hace análisis y documentación. El código se escribe o modifica solo cuando el usuario lo pide de forma explícita ("ejecuta", "hazlo", "implementa"). Preguntas como "¿podemos avanzar?" no son una orden. Antecedente: se aplicó el punto 1.5 (cancelación en Athena) sin orden explícita.

## Agregado el 2026-11-11
- **Configuración por ambiente:** un JSON en S3 con la fuente en Git, desplegado por DevOps. Los argumentos solo llevan valores de cada ejecución y apuntadores. Sin valores por defecto, una sola fuente por valor, y los tags no son configuración. La regla está en la skill: `references\configuracion-por-ambiente.md`.
- **Misma estructura de carpetas en todos los componentes, en inglés:** `domain`, `application`, `adapters/inbound`, `adapters/outbound`, `config`, `test/`. **Un solo `README.md`** por componente: argumentos, claves del JSON, salidas y qué hay en cada módulo.
- **Commits:** mensaje **muy corto en español**, sin coautor. El commit y el push los pide el usuario, normalmente al terminar una tarea ("al terminar haz commit push").
- **Orden explícita:** "aprobado, ejecuta los cambios" cuenta como orden para escribir código. Aprobar una recomendación (por ejemplo, O-2) no da permiso para crear recursos en AWS: lo de Terraform se pide a DevOps.
- **Sesiones remotas:** todo lo necesario debe quedar en el repo (`MemoriaIa\`, `CLAUDE.md` y la skill), porque la memoria local de Claude no viaja. Al cerrar un bloque de trabajo, actualizar [[traspaso-sesion]].
- **Siglas (2026-11-12):** siempre que se escriba SCD, poner entre paréntesis lo que significa: *(Slowly Changing Dimension, dimensión de cambio lento: guarda el historial de cada registro sin sobrescribirlo)*. Aplica también a otras siglas técnicas poco conocidas si el usuario lo pide.
