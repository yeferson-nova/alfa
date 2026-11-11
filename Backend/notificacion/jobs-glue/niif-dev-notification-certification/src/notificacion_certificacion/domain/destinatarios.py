from typing import Iterable

from notificacion_certificacion.domain.errores import ErrorConfiguracion


def normalizar_destinatarios(valores: Iterable[object]) -> tuple[str, ...]:
    limpios = (str(valor).strip().lower() for valor in valores)
    unicos = dict.fromkeys(valor for valor in limpios if valor)
    if not unicos:
        raise ErrorConfiguracion("NTF-CFG-003", "No hay destinatarios configurados en email.destinations")
    return tuple(unicos)
