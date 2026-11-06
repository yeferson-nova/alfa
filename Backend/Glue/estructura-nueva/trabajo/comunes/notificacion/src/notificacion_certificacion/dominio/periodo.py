import calendar
import re
from datetime import date

from notificacion_certificacion.dominio.errores import ErrorEntrada

FORMATO_PERIODO = re.compile(r"[0-9]{6}")
FORMATO_FECHA = "%d/%m/%Y"
MES_MINIMO = 1
MES_MAXIMO = 12


def rango_periodo(periodo: str) -> str:
    texto = str(periodo)
    if not FORMATO_PERIODO.fullmatch(texto):
        raise ErrorEntrada("NTF-ENT-002", f"El periodo debe tener formato AAAAMM: {texto}")
    anio, mes = int(texto[:4]), int(texto[4:])
    if not MES_MINIMO <= mes <= MES_MAXIMO:
        raise ErrorEntrada("NTF-ENT-002", f"El mes del periodo es invalido: {texto}")
    inicio = date(anio, mes, 1)
    fin = date(anio, mes, calendar.monthrange(anio, mes)[1])
    return f"{inicio.strftime(FORMATO_FECHA)} - {fin.strftime(FORMATO_FECHA)}"
