# Consultas: llaves duplicadas y vacías en Gold (7 mallas)

Antes de desplegar SCD-03 (falla si hay llaves repetidas) y SCD-04 (llaves vacías), hay que saber si alguna malla ya tiene casos en Gold. Si los tiene, la corrida fallaría con la corrección.

- Dónde correrlas: Athena, en DEV y en QA. En producción, solo lectura y las corre el equipo de producción.
- Base: `niif-<amb>-gold-catalog-db` (confirmada en DEV como `niif-dev-gold-catalog-db`; en QA y producción hay que confirmar el nombre).
- Resultado esperado: 0 filas en cada consulta. Si alguna devuelve filas, se registra la malla y se revisa antes del despliegue.
- Costo: solo leen las columnas de la llave y `act_reg`.

## auxilios-funerarios

Tabla `scd_auxilios_funerarios`. Llave: num_solicitud_tesoreria, num_poliza, tipo_identificacion_afiliado, num_identificacion_afiliado, cod_concepto, cod_origen.

```sql
SELECT num_solicitud_tesoreria, num_poliza, tipo_identificacion_afiliado, num_identificacion_afiliado, cod_concepto, cod_origen, COUNT(*) AS vigentes
FROM "niif-<amb>-gold-catalog-db"."scd_auxilios_funerarios"
WHERE act_reg = 1
GROUP BY num_solicitud_tesoreria, num_poliza, tipo_identificacion_afiliado, num_identificacion_afiliado, cod_concepto, cod_origen
HAVING COUNT(*) > 1
LIMIT 20;

SELECT COUNT(*) AS vigentes_con_llave_vacia
FROM "niif-<amb>-gold-catalog-db"."scd_auxilios_funerarios"
WHERE act_reg = 1 AND (num_solicitud_tesoreria IS NULL OR num_poliza IS NULL OR tipo_identificacion_afiliado IS NULL OR num_identificacion_afiliado IS NULL OR cod_concepto IS NULL OR cod_origen IS NULL);
```

## pcr_vida

Tabla `scd_pcr_vida`. Llave: nro_operacion, nro_poliza.

```sql
SELECT nro_operacion, nro_poliza, COUNT(*) AS vigentes
FROM "niif-<amb>-gold-catalog-db"."scd_pcr_vida"
WHERE act_reg = 1
GROUP BY nro_operacion, nro_poliza
HAVING COUNT(*) > 1
LIMIT 20;

SELECT COUNT(*) AS vigentes_con_llave_vacia
FROM "niif-<amb>-gold-catalog-db"."scd_pcr_vida"
WHERE act_reg = 1 AND (nro_operacion IS NULL OR nro_poliza IS NULL);
```

## primas-arl

Tabla `scd_primas_arl`. Llave: nid_poliza, nid_endoso.

```sql
SELECT nid_poliza, nid_endoso, COUNT(*) AS vigentes
FROM "niif-<amb>-gold-catalog-db"."scd_primas_arl"
WHERE act_reg = 1
GROUP BY nid_poliza, nid_endoso
HAVING COUNT(*) > 1
LIMIT 20;

SELECT COUNT(*) AS vigentes_con_llave_vacia
FROM "niif-<amb>-gold-catalog-db"."scd_primas_arl"
WHERE act_reg = 1 AND (nid_poliza IS NULL OR nid_endoso IS NULL);
```

## primas-endosos

Tabla `scd_primas_endosos`. Llave: nid_renta.

```sql
SELECT nid_renta, COUNT(*) AS vigentes
FROM "niif-<amb>-gold-catalog-db"."scd_primas_endosos"
WHERE act_reg = 1
GROUP BY nid_renta
HAVING COUNT(*) > 1
LIMIT 20;

SELECT COUNT(*) AS vigentes_con_llave_vacia
FROM "niif-<amb>-gold-catalog-db"."scd_primas_endosos"
WHERE act_reg = 1 AND (nid_renta IS NULL);
```

## primas-rentas

Tabla `scd_primas_rentas`. Llave: nid_renta.

```sql
SELECT nid_renta, COUNT(*) AS vigentes
FROM "niif-<amb>-gold-catalog-db"."scd_primas_rentas"
WHERE act_reg = 1
GROUP BY nid_renta
HAVING COUNT(*) > 1
LIMIT 20;

SELECT COUNT(*) AS vigentes_con_llave_vacia
FROM "niif-<amb>-gold-catalog-db"."scd_primas_rentas"
WHERE act_reg = 1 AND (nid_renta IS NULL);
```

## reserva-matematica-arp

Tabla `scd_reserva_matematica_arp`. Llave: per_corte, fec_ini_per_corte, fec_fin_per_corte, ramo, nro_poliza, nro_siniestro.

```sql
SELECT per_corte, fec_ini_per_corte, fec_fin_per_corte, ramo, nro_poliza, nro_siniestro, COUNT(*) AS vigentes
FROM "niif-<amb>-gold-catalog-db"."scd_reserva_matematica_arp"
WHERE act_reg = 1
GROUP BY per_corte, fec_ini_per_corte, fec_fin_per_corte, ramo, nro_poliza, nro_siniestro
HAVING COUNT(*) > 1
LIMIT 20;

SELECT COUNT(*) AS vigentes_con_llave_vacia
FROM "niif-<amb>-gold-catalog-db"."scd_reserva_matematica_arp"
WHERE act_reg = 1 AND (per_corte IS NULL OR fec_ini_per_corte IS NULL OR fec_fin_per_corte IS NULL OR ramo IS NULL OR nro_poliza IS NULL OR nro_siniestro IS NULL);
```

## reserva-matematica-rvi

Tabla `scd_reserva_matematica_rvi`. Llave: per_corte, fec_ini_per_corte, fec_fin_per_corte, per_pago, fec_ini_per_pago, fec_fin_per_pago, ani_per_pago, nro_poliza.

```sql
SELECT per_corte, fec_ini_per_corte, fec_fin_per_corte, per_pago, fec_ini_per_pago, fec_fin_per_pago, ani_per_pago, nro_poliza, COUNT(*) AS vigentes
FROM "niif-<amb>-gold-catalog-db"."scd_reserva_matematica_rvi"
WHERE act_reg = 1
GROUP BY per_corte, fec_ini_per_corte, fec_fin_per_corte, per_pago, fec_ini_per_pago, fec_fin_per_pago, ani_per_pago, nro_poliza
HAVING COUNT(*) > 1
LIMIT 20;

SELECT COUNT(*) AS vigentes_con_llave_vacia
FROM "niif-<amb>-gold-catalog-db"."scd_reserva_matematica_rvi"
WHERE act_reg = 1 AND (per_corte IS NULL OR fec_ini_per_corte IS NULL OR fec_fin_per_corte IS NULL OR per_pago IS NULL OR fec_ini_per_pago IS NULL OR fec_fin_per_pago IS NULL OR ani_per_pago IS NULL OR nro_poliza IS NULL);
```
