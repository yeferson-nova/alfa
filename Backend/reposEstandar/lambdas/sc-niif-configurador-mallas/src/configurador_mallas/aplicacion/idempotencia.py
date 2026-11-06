"""Idempotencia por clave: una misma invocación no produce efectos dos veces.

La clave es ``idempotencyKey`` del evento (en Scheduler, el
``<aws.scheduler.execution-id>``) o, si no llega, el ``aws_request_id``, que
Lambda conserva en sus reintentos asíncronos. Por clave:

- si ya terminó bien, se devuelve el resultado guardado sin repetir nada;
- si otra invocación la tiene tomada y su bloqueo no venció, se responde con
  un error de concurrencia (recuperable);
- si falló antes o el bloqueo venció, se vuelve a procesar.

Cada paso del caso de uso también es idempotente (escritura condicional en CNF,
mismos JSON en S3, nombre de ejecución determinista y registro EJC condicional),
así que retomar un intento a medias no duplica efectos.
"""
import hashlib

from ..dominio.errores import ErrorConcurrencia
from ..dominio.modelos import Resultado

EN_PROCESO = "EN_PROCESO"
COMPLETADO = "COMPLETADO"
FALLIDO = "FALLIDO"
_MARGEN_BLOQUEO_SEGUNDOS = 5


def ejecucion_id(malla, clave):
    """Identificador estable de la corrida: el mismo para todos sus reintentos."""
    return hashlib.sha256(f"{malla}|{clave}".encode()).hexdigest()[:12]


class ControlIdempotencia:
    def __init__(self, repositorio, ttl_segundos, reloj):
        self._repositorio = repositorio
        self._ttl = ttl_segundos
        self._reloj = reloj

    def iniciar(self, ambito, clave, tiempo_restante_ms):
        """Devuelve el resultado guardado si la clave ya se completó; si no, la toma."""
        ahora = int(self._reloj())
        bloqueo_hasta = ahora + tiempo_restante_ms // 1000 + _MARGEN_BLOQUEO_SEGUNDOS
        existente = self._repositorio.reclamar(ambito, clave, ahora, bloqueo_hasta,
                                               ahora + self._ttl)
        if existente is None:
            return None
        if existente.get("estado") == COMPLETADO and existente.get("resultado"):
            resultado = Resultado.desde_dict(existente["resultado"])
            resultado.reutilizado = True
            return resultado
        raise ErrorConcurrencia(
            "Otra invocación con la misma clave está en curso",
            codigo="CFG-CON-001",
            detalle={"bloqueoHasta": existente.get("bloqueoHasta")},
        )

    def completar(self, ambito, clave, resultado):
        self._repositorio.guardar(ambito, clave, COMPLETADO, self._expira(),
                                  resultado=resultado.to_dict())

    def fallar(self, ambito, clave, error):
        self._repositorio.guardar(ambito, clave, FALLIDO, self._expira(), error=error.to_dict())

    def _expira(self):
        return int(self._reloj()) + self._ttl
