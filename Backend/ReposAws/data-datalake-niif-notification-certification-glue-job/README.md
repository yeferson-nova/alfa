# Data Datalake NIIF - Primas Endosos - Glue Jobs

Repositorio con los scripts de AWS Glue Jobs para el caso de uso de Primas Endosos del proyecto NIIF.

## Estructura del repositorio

```
/
├── src/
│   ├── __init__.py
│   ├── jobs/
│   │   ├── bronze_primas_endosos.py      # Bronze: extracción Oracle -> S3
│   │   └── silver_primas_endosos.py      # Silver: preparación de datos
│   └── libraries/
│       └── __init__.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── jobs/
│       ├── __init__.py
│       ├── test_helpers.py
│       ├── test_bronze_primas_endosos.py
│       └── test_silver_primas_endosos.py
├── .gitignore
├── Dockerfile
├── Jenkinsfile
├── Makefile
├── README.md
├── requirements.txt
├── run_local.sh
├── sonar-project.properties
└── tox.ini
```

## Glue Jobs

| Job | Script | Descripción |
|-----|--------|-------------|
| `niif-{env}-bronze-primas-endosos` | `bronze_primas_endosos.py` | Extrae datos de Oracle (PRV/GENERAL) y los escribe en Bronze (S3 Parquet) |
| `niif-{env}-silver-primas-endosos` | `silver_primas_endosos.py` | Lee Silver base, aplica joins y filtros, escribe Silver preparada |

## Flujo de ejecución

```
Bronze (bronze_primas_endosos.py)
    ↓
Silver (silver_primas_endosos.py)
```

## Tablas extraídas

| Esquema | Tabla |
|---------|-------|
| PRV | PRV_RENTAS |
| PRV | PRV_ENDOSOS_POINT |
| GENERAL | CGR_ORIGENES |
| GENERAL | CGR_TIPOS_ENDOSOS |

## Ejecución de pruebas

```bash
# Con Docker (recomendado)
docker build -t glue-niif-primas-endosos .
docker run -v ./reports:/app/reports glue-niif-primas-endosos

# Local (requiere Python 3.8+ y Java)
pip install -r requirements.txt
pytest tests -v
```

## Despliegue

El despliegue se realiza automáticamente mediante Jenkins. El pipeline sube los scripts a S3:

- **develop** → `s3://alfa-niif-dev-assets/glue_resources/primas-endosos`
- **quality** → `s3://alfa-niif-qa-assets/glue_resources/primas-endosos`
- **main** → `s3://alfa-niif-prd-assets/glue_resources/primas-endosos`

La infraestructura (roles IAM, jobs de Glue) se gestiona en el repositorio `iac-analytics-datalake-niif-operations-manager`.
