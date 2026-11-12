NO_DATA_TEXT = "No hay datos"
QUERY_ERROR_TEXT = "Error ejecutando consulta"
NO_SUMMARY_TEXT = "No hay resumen configurado."
NO_TOTALS_TEXT = "No hay totales configurados."
NO_FILES_TEXT = "No se encontraron archivos detalle."
DEFAULT_FILE_NAME = "Archivo CSV"

REGENERATED_SUBJECT_PREFIX = "[REGENERADO] "
REGENERATION_NOTICE = (
    "CASO DE USO REEJECUTADO SIN REGISTROS NUEVOS\n"
    "Archivos regenerados desde Gold existente del período.\n"
    "Silver y Gold no se ejecutaron nuevamente.\n"
    "No se verificaron cambios en registros ya procesados."
)

BODY_TEMPLATE = (
    "\nSolicitud de certificación NIIF 17\n\n"
    "Caso de uso:\n{use_case_name}\n\n"
    "Resumen:\n{summary}\n\n"
    "Totales:\n{totals}\n\n"
    "Archivos generados:\n{files}\n\n"
    "Certificar:\n{certify_link}\n\n"
    "No certificar:\n{no_certify_link}\n\n"
    "Este mensaje fue generado automáticamente por el flujo NIIF 17.\n"
)

AVAILABILITY_HEADER = "Disponibilidad por caso de uso:"
MEMBER_NO_DATA_LINE = "{name}: no se encontraron datos activos para el periodo {period}."
NOTICE_NO_MEMBER = "Ninguno de los casos del compuesto tiene datos activos para el periodo solicitado."
NOTICE_OTHER_MEMBERS = "El archivo y los resumenes incluyen los datos disponibles de los demas casos."

NO_DATA_TOTAL_NOTICE = "No se encontraron datos procesados para el periodo seleccionado: {period}."
NO_DATA_PARTIAL_NOTICE = (
    "No se encontraron datos procesados para el periodo {period} en las consultas: {queries}. "
    "Las demas consultas conservan sus datos."
)
