"""Catálogo de mallas: lo que hoy está quemado en cada copia del configurador.

Es un documento JSON versionado (ver ``config/catalogo-mallas.<ambiente>.json``).
Para sumar una malla se agrega una entrada; no se copia código. Este módulo solo
valida y convierte el documento; la lectura la hace un adaptador.
"""
import re
from dataclasses import dataclass

from .errores import ErrorConfiguracion
from .modelos import MallaSpec, ReglasPeriodo

_CODIGO = re.compile(r"[A-Z][A-Z0-9_]{1,62}")
_PREFIJO = re.compile(r"[a-z0-9][a-z0-9-]{0,29}")
_NOMBRE_MAQUINA = re.compile(r"[A-Za-z0-9_-]{1,80}")
_COLUMNA = re.compile(r"[A-Za-z_][A-Za-z_0-9]{0,127}")


@dataclass(frozen=True)
class Catalogo:
    version: str
    modos_ejecucion: tuple[str, ...]
    mallas: dict

    def obtener(self, codigo):
        try:
            return self.mallas[codigo]
        except KeyError:
            raise ErrorConfiguracion(
                f"La malla {codigo} no está en el catálogo",
                codigo="CFG-CNF-003",
                detalle={"disponibles": sorted(self.mallas)},
            ) from None


def parsear(documento):
    if not isinstance(documento, dict):
        _invalido("el documento debe ser un objeto")
    version = _texto(documento, "version", "catálogo")
    modos = _lista_textos(documento, "modosEjecucion", "catálogo")
    mallas = documento.get("mallas")
    if not isinstance(mallas, dict) or not mallas:
        _invalido("'mallas' debe ser un objeto con al menos una malla")
    return Catalogo(
        version=version,
        modos_ejecucion=modos,
        mallas={codigo: _malla(codigo, datos) for codigo, datos in mallas.items()},
    )


def _malla(codigo, datos):
    if not _CODIGO.fullmatch(codigo):
        _invalido(f"código de malla inválido: {codigo}")
    if not isinstance(datos, dict):
        _invalido(f"{codigo}: la definición debe ser un objeto")
    programacion = _objeto(datos, "programacion", codigo)
    periodo = _objeto(datos, "periodo", codigo)
    total_ciclos = datos.get("totalCiclos")
    if isinstance(total_ciclos, bool) or not isinstance(total_ciclos, int) or total_ciclos < 1:
        _invalido(f"{codigo}: totalCiclos debe ser un entero mayor que cero")
    incluir_rango = datos.get("incluirRangoFechasEnEntrada")
    if not isinstance(incluir_rango, bool):
        _invalido(f"{codigo}: incluirRangoFechasEnEntrada debe ser true o false")

    return MallaSpec(
        codigo=codigo,
        nombre_caso=_texto(datos, "nombreCaso", codigo),
        prefijo_ejecucion=_con_patron(datos, "prefijoEjecucion", codigo, _PREFIJO),
        state_machine=_con_patron(datos, "stateMachineName", codigo, _NOMBRE_MAQUINA),
        prg_pk=_texto(programacion, "pk", f"{codigo}.programacion"),
        prg_sk=_texto(programacion, "sk", f"{codigo}.programacion"),
        total_ciclos=total_ciclos,
        estados_remediacion=_lista_textos(datos, "estadosRemediacionPermitidos", codigo),
        reglas_periodo=_reglas_periodo(codigo, periodo),
        incluir_rango_fechas=incluir_rango,
    )


def _reglas_periodo(codigo, periodo):
    columnas = periodo.get("columnasFecha")
    if not isinstance(columnas, list) or not all(
            isinstance(c, str) and _COLUMNA.fullmatch(c) for c in columnas):
        _invalido(f"{codigo}: periodo.columnasFecha debe ser una lista de nombres de columna")
    columna_periodo = periodo.get("columnaPeriodo")
    if columna_periodo is not None and (
            not isinstance(columna_periodo, str) or not _COLUMNA.fullmatch(columna_periodo)):
        _invalido(f"{codigo}: periodo.columnaPeriodo inválida")
    patron = periodo.get("patronIdentificadorSegmento")
    if patron is not None:
        try:
            compilado = re.compile(patron)
        except (re.error, TypeError):
            _invalido(f"{codigo}: periodo.patronIdentificadorSegmento no es una expresión válida")
        if compilado.groups != 1:
            _invalido(f"{codigo}: patronIdentificadorSegmento debe tener un solo grupo (el periodo)")
    return ReglasPeriodo(tuple(columnas), columna_periodo, patron)


def _objeto(datos, nombre, contexto):
    valor = datos.get(nombre)
    if not isinstance(valor, dict):
        _invalido(f"{contexto}: '{nombre}' debe ser un objeto")
    return valor


def _texto(datos, nombre, contexto):
    valor = datos.get(nombre)
    if not isinstance(valor, str) or not valor.strip():
        _invalido(f"{contexto}: '{nombre}' debe ser un texto no vacío")
    return valor


def _con_patron(datos, nombre, contexto, patron):
    valor = _texto(datos, nombre, contexto)
    if not patron.fullmatch(valor):
        _invalido(f"{contexto}: '{nombre}' tiene un formato inválido")
    return valor


def _lista_textos(datos, nombre, contexto):
    valor = datos.get(nombre)
    if not isinstance(valor, list) or not valor or not all(
            isinstance(v, str) and v.strip() for v in valor):
        _invalido(f"{contexto}: '{nombre}' debe ser una lista de textos no vacía")
    return tuple(valor)


def _invalido(motivo):
    raise ErrorConfiguracion(f"Catálogo de mallas inválido: {motivo}", codigo="CFG-CNF-002")
