from notificacion_certificacion.domain.constantes import (
    ESTADO_LISTO_PARA_NOTIFICAR,
    ESTADO_NOTIFICACION_COMPLETADA,
    ESTADO_NOTIFICACION_EN_CURSO,
)
from notificacion_certificacion.domain.errores import ErrorConcurrencia
from notificacion_certificacion.domain.modelos import ConfiguracionCompuesta, RegistroCompuesto

SEPARADOR_IDENTIFICADOR = "#"


def identificador_compuesto(configuracion: ConfiguracionCompuesta, periodo: str) -> str:
    return f"{configuracion.id_grupo}{SEPARADOR_IDENTIFICADOR}{periodo}"


def control_ya_completado(control: RegistroCompuesto | None, cert_code: str) -> bool:
    if control is not None and control.estado == ESTADO_NOTIFICACION_COMPLETADA:
        if control.cert_code == cert_code:
            return True
        raise ErrorConcurrencia("NTF-CON-001", "Control ya completado para otra solicitud; no se reenvia")
    reservado = control is not None and control.estado == ESTADO_NOTIFICACION_EN_CURSO
    if not reservado or not control.actualizado_en:
        raise ErrorConcurrencia("NTF-CON-001", "Control no esta reservado para notificacion")
    return False


def validar_miembro(miembro: RegistroCompuesto | None, id_miembro: str, periodo: str) -> RegistroCompuesto:
    listo = miembro is not None and miembro.estado == ESTADO_LISTO_PARA_NOTIFICAR
    if not listo or str(miembro.periodo) != str(periodo):
        raise ErrorConcurrencia("NTF-CON-001", f"Miembro no listo: {id_miembro}")
    if not (miembro.id_ejecucion_origen and miembro.id_lote and miembro.actualizado_en):
        raise ErrorConcurrencia("NTF-CON-001", f"Miembro sin identidad completa: {id_miembro}")
    return miembro
