-- Confirma en Athena DEV dónde se pierde Pymes (55139). Solo lectura, sin datos de personas.
-- Lote 20261124060016 (Emisiones 202606). Workgroup: primary. Base: niif-dev-bronze-catalog-db.
--   movimientos Generales: data_e3b30b04994088555abb95f4054d5896
--   agregatedpolicy (tabla compartida por los 3 casos): data_aab5b25cd0ab13890a1be630de8f754f
-- Esperado si la hipótesis es cierta:
--   Q-A: 468 movimientos sin póliza en el Bronce, total 188114812.96 (igual a Q5 de Pymes en la Oracle).
--   Q-B: 0 pólizas del producto 55139 en el agregatedpolicy compartido.
-- Correr cada consulta por separado.

-- Q-A. Movimientos de Generales cuya póliza no está en el agregatedpolicy compartido.
SELECT COUNT(*) AS movimientos_sin_poliza,
       COUNT(DISTINCT t.policyid) AS polizas,
       COUNT(DISTINCT t.operationpk) AS operaciones,
       SUM(CAST(t.ammount AS decimal(38,6))) AS total
FROM "niif-dev-bronze-catalog-db"."data_e3b30b04994088555abb95f4054d5896" t
WHERE NOT EXISTS (
  SELECT 1 FROM "niif-dev-bronze-catalog-db"."data_aab5b25cd0ab13890a1be630de8f754f" ap
  WHERE CAST(ap.agregatedpolicyid AS varchar) = CAST(t.policyid AS varchar));

-- Q-B. Pólizas por producto de Generales en el agregatedpolicy compartido (55139 debería faltar).
SELECT CAST(productid AS varchar) AS productid, COUNT(DISTINCT agregatedpolicyid) AS polizas
FROM "niif-dev-bronze-catalog-db"."data_aab5b25cd0ab13890a1be630de8f754f"
WHERE CAST(productid AS varchar) IN ('55059','54999','54959','52897','54979','55099','55139')
GROUP BY 1 ORDER BY 1;

-- Q-C. Conceptos de esos movimientos sin póliza (deben ser los 69 de Pymes en Q5).
SELECT upper(t.concept) AS concepto, COUNT(*) AS filas, SUM(CAST(t.ammount AS decimal(38,6))) AS total
FROM "niif-dev-bronze-catalog-db"."data_e3b30b04994088555abb95f4054d5896" t
WHERE NOT EXISTS (
  SELECT 1 FROM "niif-dev-bronze-catalog-db"."data_aab5b25cd0ab13890a1be630de8f754f" ap
  WHERE CAST(ap.agregatedpolicyid AS varchar) = CAST(t.policyid AS varchar))
GROUP BY 1 ORDER BY 1;
