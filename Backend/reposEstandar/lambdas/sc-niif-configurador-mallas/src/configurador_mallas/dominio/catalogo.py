"""Catálogo de mallas: lo que hoy está quemado en cada copia del configurador.

Es un documento JSON versionado (ver ``config/catalogo-mallas.<ambiente>.json``).
Para sumar una malla se agrega una entrada; no se copia código. Este módulo solo
valida y convierte el documento; la lectura la hace un adaptador.
"""
import re
from dataclasses import dataclass

from .errores import ErrorConfiguracion
from .modelos import (EntradaOrquestacion, FormatoTablaExtraccion, MallaSpec, ReglasPeriodo,
                      ReglasRemediacion)

_CODIGO = re.compile(r"[A-Z][A-Z0-9_]{1,62}")
_PREFIJO = re.compile(r"[a-z0-9][a-z0-9-]{0,29}")
_NOMBRE_MAQUINA = re.compile(r"[A-Za-z0-9_-]{1,80}")
_COLUMNA = re.compile(r"[A-Za-z_][A-Za-z_0-9]{0,127}")
_CAMPO = re.compile(r"[A-Za-z_][A-Za-z_0-9]{0,63}")
OPERADORES_FECHA = (">=", "<", "<=")


@dataclass(frozen=True)
class Catalogo:
    version: str
    modos_ejecucion: tuple[str, ...]
    modo_por_defecto: str
    origen_por_defecto: str
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
    defectos = _objeto(documento, "valoresPorDefecto", "catálogo")
    modo_por_defecto = _texto(defectos, "modoEjecucion", "catálogo.valoresPorDefecto")
    if modo_por_defecto not in modos:
        _invalido("valoresPorDefecto.modoEjecucion debe estar en modosEjecucion")
    mallas = documento.get("mallas")
    if not isinstance(mallas, dict) or not mallas:
        _invalido("'mallas' debe ser un objeto con al menos una malla")
    return Catalogo(
        version=version,
        modos_ejecucion=modos,
        modo_por_defecto=modo_por_defecto,
        origen_por_defecto=_texto(defectos, "origen", "catálogo.valoresPorDefecto"),
        mallas={codigo: _malla(codigo, datos) for codigo, datos in mallas.items()},
    )


def _malla(codigo, datos):
    if not _CODIGO.fullmatch(codigo):
        _invalido(f"código de malla inválido: {codigo}")
    if not isinstance(datos, dict):
        _invalido(f"{codigo}: la definición debe ser un objeto")
    programacion = _objeto(datos, "programacion", codigo)
    total_ciclos = datos.get("totalCiclos")
    if isinstance(total_ciclos, bool) or not isinstance(total_ciclos, int) or total_ciclos < 1:
        _invalido(f"{codigo}: totalCiclos debe ser un entero mayor que cero")

    return MallaSpec(
        codigo=codigo,
        nombre_caso=_texto(datos, "nombreCaso", codigo),
        prefijo_ejecucion=_con_patron(datos, "prefijoEjecucion", codigo, _PREFIJO),
        state_machine=_con_patron(datos, "stateMachineName", codigo, _NOMBRE_MAQUINA),
        prg_pk=_texto(programacion, "pk", f"{codigo}.programacion"),
        prg_sk=_texto(programacion, "sk", f"{codigo}.programacion"),
        total_ciclos=total_ciclos,
        remediacion=_remediacion(codigo, _objeto(datos, "remediacion", codigo)),
        reglas_periodo=_reglas_periodo(codigo, _objeto(datos, "periodo", codigo)),
        formato_tabla=_formato_tabla(codigo, _objeto(datos, "tablaExtraccion", codigo)),
        entrada_orquestacion=_entrada_orquestacion(
            codigo, _objeto(datos, "entradaStepFunction", codigo)),
    )


def _remediacion(codigo, datos):
    contexto = f"{codigo}.remediacion"
    con_reglas = datos.get("estadoConReglas")
    if con_reglas is not None:
        con_reglas = _texto(datos, "estadoConReglas", contexto)
    return ReglasRemediacion(_texto(datos, "estadoSinReglas", contexto), con_reglas)


def _reglas_periodo(codigo, periodo):
    columnas = periodo.get("columnasFecha")
    if not isinstance(columnas, list) or not all(
            isinstance(c, str) and _COLUMNA.fullmatch(c) for c in columnas):
        _invalido(f"{codigo}: periodo.columnasFecha debe ser una lista de nombres de columna")
    operadores = periodo.get("operadoresFecha")
    if not isinstance(operadores, list) or not all(o in OPERADORES_FECHA for o in operadores):
        _invalido(f"{codigo}: periodo.operadoresFecha debe ser una lista con "
                  f"valores de {list(OPERADORES_FECHA)}")
    if columnas and not operadores:
        _invalido(f"{codigo}: periodo.operadoresFecha no puede estar vacía si hay columnasFecha")
    columna_periodo = periodo.get("columnaPeriodo")
    if columna_periodo is not None and (
            not isinstance(columna_periodo, str) or not _COLUMNA.fullmatch(columna_periodo)):
        _invalido(f"{codigo}: periodo.columnaPeriodo inválida")
    return ReglasPeriodo(
        columnas_fecha=tuple(columnas),
        operadores_fecha=tuple(operadores),
        columna_periodo=columna_periodo,
        ajustar_segmentacion=_booleano(periodo, "ajustarSegmentacion", f"{codigo}.periodo"),
        patron_segmento=_patron_segmento(codigo, periodo.get("patronIdentificadorSegmento")),
    )


def _patron_segmento(codigo, patron):
    if patron is None:
        return None
    try:
        compilado = re.compile(patron)
    except (re.error, TypeError):
        _invalido(f"{codigo}: periodo.patronIdentificadorSegmento no es una expresión válida")
    if compilado.groups != 1:
        _invalido(f"{codigo}: patronIdentificadorSegmento debe tener un solo grupo (el periodo)")
    return patron


def _formato_tabla(codigo, datos):
    campos = datos.get("camposOpcionales")
    if not isinstance(campos, dict) or not all(
            isinstance(v, str) and _CAMPO.fullmatch(k) and _CAMPO.fullmatch(v)
            for k, v in campos.items()):
        _invalido(f"{codigo}: tablaExtraccion.camposOpcionales debe ser un objeto "
                  "{campoCnf: campo_json}")
    return FormatoTablaExtraccion(
        campos_opcionales=tuple(campos.items()),
        omitir_nulos=_booleano(datos, "omitirNulos", f"{codigo}.tablaExtraccion"),
    )


def _entrada_orquestacion(codigo, datos):
    contexto = f"{codigo}.entradaStepFunction"
    return EntradaOrquestacion(
        incluir_ruta_config_extraccion=_booleano(datos, "incluirRutaConfigExtraccion", contexto),
        incluir_rango_fechas=_booleano(datos, "incluirRangoFechas", contexto),
    )


def _booleano(datos, nombre, contexto):
    valor = datos.get(nombre)
    if not isinstance(valor, bool):
        _invalido(f"{contexto}: '{nombre}' debe ser true o false")
    return valor


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
