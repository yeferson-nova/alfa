"""
catalogo — Lectura del DM_CATALOGO.

Punto único de entrada para acceder a las tablas del catálogo
(`tbl_dominio`, `tbl_atributo`, `tbl_regla`, `tbl_regla_atributo`, `tbl_umbral`).

Modo local  → lee SQLite en tests/local_catalog/dm_catalogo.db
Modo AWS    → lee Glue Catalog (database dm_catalogo_db)

El switch se decide en runtime via `lib.config.get_mode()`.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lib import config

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class ReglaConfig:
    """Una regla configurada en el catálogo (atributo + regla + parámetros)."""

    id_regla: int
    codigo: str
    dimension: str
    patron_dqdl: str
    parametros: str
    id_atributo: int
    nombre_atributo: str
    tabla_fisica: str
    columna_fisica: str
    tipo_dato: str


# ────────────────────────────────────────────────────────────────────
# API pública — usada por jobs y librería
# ────────────────────────────────────────────────────────────────────


def leer_reglas_activas(
    spark: SparkSession,
    dominio: str,
    dimension: str = "COMPLETITUD",
) -> list[ReglaConfig]:
    """
    Devuelve la lista de reglas activas para un (dominio, dimensión).

    Args:
        spark: SparkSession (no se usa en modo local con SQLite, pero se pasa por
            consistencia con el modo AWS donde Spark consulta el Glue Catalog).
        dominio: 'clientes' | 'polizas' | 'productos' | 'siniestros'
        dimension: 'COMPLETITUD' | 'VALIDEZ' | 'UNICIDAD' | 'EXACTITUD'

    Returns:
        Lista de ReglaConfig. Vacía si no hay matches.

    Raises:
        ValueError: si dominio o dimension no son válidos.
    """
    if dominio not in config.DOMINIOS:
        raise ValueError(f"Dominio inválido: {dominio}. Válidos: {config.DOMINIOS}")
    if dimension not in config.DIMENSIONES:
        raise ValueError(f"Dimensión inválida: {dimension}. Válidos: {config.DIMENSIONES}")

    if config.is_local():
        return _leer_desde_sqlite(dominio, dimension)
    return _leer_desde_glue_catalog(spark, dominio, dimension)


def leer_atributos_criticos(
    spark: SparkSession,
    dominio: str,
) -> list[dict]:
    """
    Devuelve los atributos marcados como críticos para el dominio.

    Returns:
        Lista de dicts con keys:
        [id_atributo, nombre, tabla_fisica, columna_fisica, tipo_dato]
    """
    if config.is_local():
        return _atributos_criticos_sqlite(dominio)
    return _atributos_criticos_glue(spark, dominio)


# ────────────────────────────────────────────────────────────────────
# Implementación LOCAL (SQLite)
# ────────────────────────────────────────────────────────────────────


def _leer_desde_sqlite(dominio: str, dimension: str) -> list[ReglaConfig]:
    """Query al simulador SQLite del Glue Catalog."""
    if not config.LOCAL_CATALOG_DB.exists():
        raise FileNotFoundError(
            f"SQLite no existe: {config.LOCAL_CATALOG_DB}. "
            f"Corré primero: python scripts/init_local_catalog.py"
        )

    sql = """
        SELECT
            r.id_regla,
            r.codigo,
            r.dimension,
            r.patron_dqdl,
            COALESCE(r.parametros, '{}') AS parametros,
            a.id_atributo,
            a.nombre AS nombre_atributo,
            a.tabla_fisica,
            a.columna_fisica,
            a.tipo_dato
        FROM tbl_regla_atributo ra
        JOIN tbl_regla r ON ra.id_regla = r.id_regla
        JOIN tbl_atributo a ON ra.id_atributo = a.id_atributo
        JOIN tbl_dominio d ON a.id_dominio = d.id_dominio
        WHERE d.nombre = ?
          AND r.dimension = ?
          AND r.activa = 1
          AND ra.activa = 1
          AND a.es_critico = 1
        ORDER BY a.nombre, r.codigo
    """
    with sqlite3.connect(config.LOCAL_CATALOG_DB) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, (dominio, dimension)).fetchall()

    return [ReglaConfig(**dict(row)) for row in rows]


def _atributos_criticos_sqlite(dominio: str) -> list[dict]:
    sql = """
        SELECT
            a.id_atributo,
            a.nombre,
            a.tabla_fisica,
            a.columna_fisica,
            a.tipo_dato
        FROM tbl_atributo a
        JOIN tbl_dominio d ON a.id_dominio = d.id_dominio
        WHERE d.nombre = ? AND a.es_critico = 1
        ORDER BY a.nombre
    """
    with sqlite3.connect(config.LOCAL_CATALOG_DB) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, (dominio,)).fetchall()
    return [dict(r) for r in rows]


# ────────────────────────────────────────────────────────────────────
# Implementación AWS (Glue Catalog)
# ────────────────────────────────────────────────────────────────────


def _leer_desde_glue_catalog(
    spark: SparkSession, dominio: str, dimension: str
) -> list[ReglaConfig]:
    """
    Query al Glue Catalog vía Spark SQL.

    Requiere grants Lake Formation: DESCRIBE + SELECT sobre dm_catalogo_db.
    """
    db = config.AWSPaths.CATALOGO_DB
    sql = f"""
        SELECT
            r.id_regla,
            r.codigo,
            r.dimension,
            r.patron_dqdl,
            COALESCE(r.parametros, '{{}}') AS parametros,
            a.id_atributo,
            a.nombre AS nombre_atributo,
            a.tabla_fisica,
            a.columna_fisica,
            a.tipo_dato
        FROM {db}.tbl_regla_atributo ra
        JOIN {db}.tbl_regla r ON ra.id_regla = r.id_regla
        JOIN {db}.tbl_atributo a ON ra.id_atributo = a.id_atributo
        JOIN {db}.tbl_dominio d ON a.id_dominio = d.id_dominio
        WHERE d.nombre = '{dominio}'
          AND r.dimension = '{dimension}'
          AND r.activa = TRUE
          AND ra.activa = TRUE
          AND a.es_critico = TRUE
        ORDER BY a.nombre, r.codigo
    """
    rows = spark.sql(sql).collect()
    return [
        ReglaConfig(
            id_regla=row["id_regla"],
            codigo=row["codigo"],
            dimension=row["dimension"],
            patron_dqdl=row["patron_dqdl"],
            parametros=row["parametros"],
            id_atributo=row["id_atributo"],
            nombre_atributo=row["nombre_atributo"],
            tabla_fisica=row["tabla_fisica"],
            columna_fisica=row["columna_fisica"],
            tipo_dato=row["tipo_dato"],
        )
        for row in rows
    ]


def _atributos_criticos_glue(spark: SparkSession, dominio: str) -> list[dict]:
    db = config.AWSPaths.CATALOGO_DB
    sql = f"""
        SELECT
            a.id_atributo,
            a.nombre,
            a.tabla_fisica,
            a.columna_fisica,
            a.tipo_dato
        FROM {db}.tbl_atributo a
        JOIN {db}.tbl_dominio d ON a.id_dominio = d.id_dominio
        WHERE d.nombre = '{dominio}' AND a.es_critico = TRUE
        ORDER BY a.nombre
    """
    rows = spark.sql(sql).collect()
    return [row.asDict() for row in rows]
