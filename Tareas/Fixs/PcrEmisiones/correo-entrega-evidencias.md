# Correo de entrega de evidencias: PCR Emisiones junio 2026

Versión final del correo (2026-11-24). Va sin el tablero; adjunta las evidencias como respaldo. "Silver en AWS (DEV y QA)" es nuestro resultado; "Oracle" es el origen ACSELE que usan DEV y QA; el RRC es la cifra oficial.

---

**Asunto:** PCR Emisiones junio 2026: resultado de la validación y entrega de evidencias

Hola a todos:

Les comparto el resultado de la validación que hice de PCR Emisiones de junio 2026 frente al RRC, que es la cifra oficial. Adjunto las evidencias para que queden como respaldo.

**Resumen**
- Comparé lo cargado con la tabla de movimientos de la Oracle ACSELE (`THIRDPARTYMOVEMENTPOLICY`, base que usan DEV y QA). En 12 productos coinciden filas y montos al centavo, en las 293 combinaciones de producto y concepto.
- DEV y QA tienen los mismos datos.
- El 99,95 % de la prima cuadra con el RRC.
- Satélites y Vida cuadran en prima y comisiones. Generales cuadra en prima y recaudo en 41 de 45 acuerdos.
- Encontré **3 diferencias**, que explico abajo. En las tres, los valores están en la tabla de movimientos de la Oracle; la diferencia aparece después, en la carga o en el cálculo del Silver en AWS (DEV y QA).

**Diferencias encontradas**

| # | Hallazgo | Valor en el RRC | Valor en el Silver en AWS | Diferencia |
|---|---|---|---|---|
| 1 | Pymes no llega al reporte (prima) | 56.404.532,99 | 0 | −56.404.532,99 |
| 2 | Al cálculo de Pymes le faltan conceptos (prima) | 56.404.532,99 | 15.326.346,61 | −41.078.186,38 |
| 3 | IVA de la comisión de recaudo de Generales | 702.183.120,50 | 0 | −702.183.120,50 |

**1. Pymes no llega al reporte**
- **Lo que hay en la Oracle:** en la tabla de movimientos (`THIRDPARTYMOVEMENTPOLICY`) hay 468 movimientos de Pymes de junio, de 45 operaciones. Sumados por concepto dan exactamente el RRC:
  - prima: 56.404.532,99;
  - comisión de recaudo: 11.280.902,12;
  - IVA de recaudo: 2.143.371,37.
- **Dónde se pierde:** la tabla de pólizas (`AGREGATEDPOLICY`), que comparten Generales, Vida y Satélites, se carga sin las pólizas de Pymes. Sin la póliza, el Silver en AWS no puede ubicar los 468 movimientos y los deja por fuera.
- **Cómo lo confirmé en Athena DEV:**
  - los 468 movimientos están cargados pero sin póliza;
  - la tabla de pólizas cargada no tiene ninguna de Pymes;
  - los 69 conceptos de esos movimientos son iguales a los de la Oracle, en filas y monto al centavo.

**2. Al cálculo de Pymes le faltan conceptos**
- El cálculo del Silver en AWS solo reconoce los conceptos de sustracción de Pymes. Faltan incendio, terremoto, equipo electrónico y rotura, con sus devoluciones, y todo el IVA de recaudo.
- Aunque la carga trajera todo, la prima quedaría en 15.326.346,61, el 27 % de lo esperado.

**3. El IVA de la comisión de recaudo de Generales sale en cero**
- Pasa en Deudores (código del producto 52897: 633.791.830,29 en el RRC) y Protección Financiera (código del producto 55059: 68.391.290,21 en el RRC).
- La tabla de movimientos de la Oracle (`THIRDPARTYMOVEMENTPOLICY`) sí trae esos valores, pero con conceptos que el cálculo del Silver en AWS no tiene incluidos.
- Los sumé con la misma regla de signos que el Silver ya usa para la prima y la comisión de recaudo: suman base y rehabilitación; restan devolución y anulación.

*Deudores (52897)*

| Cobertura | Base | Rehabilitación | Devolución | Anulación |
|---|---|---|---|---|
| Incendio | `IVACOSTORECAUDOINCE` | `IVACOSTORECAUDOINCREHAB` | `IVACOSTORECAUDOINCEDEV` | `IVACOSTORECAUDOINCANU` |
| Terremoto | `IVACOSTORECAUDOTERR` | `IVACOSTORECAUDOTERRREHAB` | `IVACOSTORECAUDOTERRDEV` | `IVACOSTORECAUDOTERRANU` |
| Responsabilidad civil | `IVACOSTORECAUDORC` | `IVACOSTORECAUDORCREHAB` | `IVACOSTORECAUDORCDEV` | `IVACOSTORECAUDORCANU` |

Con esta regla, la Oracle da 633.791.879,69 frente a 633.791.830,29 del RRC. La diferencia de 49,40 aún no tiene explicación y la revisaré con negocio.

*Protección Financiera (55059)*

Este producto solo tiene base y devolución.

| Cobertura | Base | Devolución |
|---|---|---|
| Accidentes | `IVACOMISIONRECAUDOACC` | `IVACOMISIONRECAUDOACCDEV` |
| Desempleo | `IVACOMISIONRECAUDODES` | `IVACOMISIONRECAUDODESDEV` |
| Sustracción | `IVACOMISIONRECAUDOSUS` | `IVACOMISIONRECAUDOSUSDEV` |

Con esta regla, la Oracle da 68.391.290,21, igual al RRC al centavo.

**Otros puntos (no son fallas de carga)**
- El IVA de la comisión del intermediario (243.494.152,25) no existe como concepto en la tabla de movimientos de ACSELE. Hoy lo calcula negocio con el 19 % de la comisión.
- **Diferencias menores frente al RRC:**
  - **Deudores (52897), acuerdo INC-27192:** el Silver en AWS (DEV y QA) trae 259,98 más de prima y 260,00 más de comisión de recaudo que el RRC. El Silver coincide con la Oracle. La causa de la diferencia aún no está identificada.
  - **Vida Deudores (48897):** el Silver en AWS trae 42.900,00 más de comisión de recaudo y 8.151,00 más de su IVA que el RRC. El valor del Silver es igual al del reporte E1-A1 de negocio; la diferencia está entre el RRC y E1-A1.
  - **Vida AP (54919):** el Silver en AWS trae 4.053,80 menos de comisión que el RRC (53.666,30 contra 57.720,10). El Silver descuenta la devolución de comisión que registra la Oracle (`COMISIONVIDAAPDEV`) y el RRC no la descuenta.

**Alcance:** la validación la hice en DEV y QA, contra la misma Oracle ACSELE. PRD no está incluida.

**Evidencias adjuntas**
- Resultados de las consultas en la Oracle: movimientos por producto y concepto (`sql5.xlsx`), movimientos por día (`sql6.xlsx`) y el diagnóstico de Pymes (capturas).
- Resultado de la consulta en Athena DEV que confirma los 468 movimientos de Pymes sin póliza (`.csv`).
- Paquetes de evidencia de las corridas del diagnóstico en DEV y QA.

Quedo atento a sus comentarios.

Saludos,
