# Código de referencia (NO es el código desplegado)

Para las lambdas `niif-dev-configuration-<malla>` cuyo código no se pudo extraer (disco lleno en CloudShell), aquí hay una **copia** del `lambda_function.py` de la lambda anterior `niif-dev-configurador-<malla>` (original en `..\..\pruebas-lambda\codigo\`).

- Medido con auxilios-funerarios: la `configuration` es la `configurador` más ~120 líneas (92,3 % igual). Añade `_evento_desde_prg` (lee la programación en `prg-programaciones`) y `_resolver_where_periodo` (fechas del periodo en los filtros).
- Por eso estas copias **no incluyen esas dos funciones**; el comportamiento real de la `configuration` puede diferir.
- Reemplazar por el código real cuando se extraiga (`cloudshell-inventario\inventario.sh`).

Copiadas: primas-rentas, primas-endosos, primas-arl, reserva-matematica-rvi, reserva-matematica-arp.
Sin referencia: `niif-dev-manager-configuration` (no hay equivalente local; el programador anterior es `niif-dev-programador-configuraciones`).
