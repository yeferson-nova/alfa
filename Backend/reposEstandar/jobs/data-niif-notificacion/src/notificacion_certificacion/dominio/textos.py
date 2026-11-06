TEXTO_SIN_DATOS = "No hay datos"
TEXTO_ERROR_CONSULTA = "Error ejecutando consulta"
TEXTO_SIN_RESUMEN = "No hay resumen configurado."
TEXTO_SIN_TOTALES = "No hay totales configurados."
TEXTO_SIN_ARCHIVOS = "No se encontraron archivos detalle."
TEXTO_ARCHIVO_POR_DEFECTO = "Archivo CSV"

PREFIJO_ASUNTO_REGENERADO = "[REGENERADO] "
AVISO_REGENERACION = (
    "CASO DE USO REEJECUTADO SIN REGISTROS NUEVOS\n"
    "Archivos regenerados desde Gold existente del período.\n"
    "Silver y Gold no se ejecutaron nuevamente.\n"
    "No se verificaron cambios en registros ya procesados."
)

PLANTILLA_CUERPO = (
    "\nSolicitud de certificación NIIF 17\n\n"
    "Caso de uso:\n{caso}\n\n"
    "Resumen:\n{resumen}\n\n"
    "Totales:\n{totales}\n\n"
    "Archivos generados:\n{archivos}\n\n"
    "Certificar:\n{certificar}\n\n"
    "No certificar:\n{no_certificar}\n\n"
    "Este mensaje fue generado automáticamente por el flujo NIIF 17.\n"
)

ENCABEZADO_DISPONIBILIDAD = "Disponibilidad por caso de uso:"
LINEA_SIN_DATOS_MIEMBRO = "{nombre}: no se encontraron datos activos para el periodo {periodo}."
AVISO_NINGUN_MIEMBRO = "Ninguno de los casos del compuesto tiene datos activos para el periodo solicitado."
AVISO_DEMAS_MIEMBROS = "El archivo y los resumenes incluyen los datos disponibles de los demas casos."

AVISO_SIN_DATOS_TOTAL = "No se encontraron datos procesados para el periodo seleccionado: {periodo}."
AVISO_SIN_DATOS_PARCIAL = (
    "No se encontraron datos procesados para el periodo {periodo} en las consultas: {consultas}. "
    "Las demas consultas conservan sus datos."
)
