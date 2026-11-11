import os
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

REDIRECT_BASE_URL = os.environ["REDIRECT_BASE_URL"]

SHORT_LINKS_TABLE = os.environ["SHORT_LINKS_TABLE"]


def get_s(new_image, field_name, default=""):
    return new_image.get(field_name, {}).get("S", default)


def lambda_handler(event, context):
    for record in event.get("Records", []):
        try:
            if record.get("eventName") != "MODIFY":
                continue

            new = record.get("dynamodb", {}).get("NewImage")
            if not new:
                continue

            new_status = new["status"]["S"]

            logger.info("---- EVENTO DYNAMODB ----")
            logger.info(new)
            logger.info("NEW STATUS: %s", new_status)

            # Solo cuando está en PENDING
            if new_status != "PENDING":
                continue

            # Evitar reprocesar si ya existe sendLock
            # Igual que ambiente: solo valida, NO lo crea.
            if "sendLock" in new:
                logger.info("Registro ya procesado (sendLock presente), no dispara Glue")
                continue

            use_case_id = new["useCaseId"]["S"]
            cert_code = new["certCode"]["S"]
            email_type = new.get("emailType", {}).get("S", "")
            query_name = new.get("queryName", {}).get("S", "")

            if query_name != "detail#1":
                logger.info(
                    f"Evento PENDING ignorado para queryName={query_name}. "
                    "Solo dispara detail#1."
                )
                continue

            config_bucket = get_s(new, "configBucket")
            config_key = get_s(new, "configKey")
            global_config_key = get_s(new, "globalConfigKey")

            if not config_bucket or not config_key or not global_config_key:
                logger.info(
                    "Evento PENDING sin configBucket/configKey/globalConfigKey. "
                    "No dispara Glue. certCode=%s",
                    cert_code
                )
                continue

            logger.info(
                f"Disparando Glue Job para useCaseId={use_case_id}, "
                f"certCode={cert_code}, configKey={config_key}"
            )

            response = glue.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    "--useCaseId": use_case_id,
                    "--certCode": cert_code,
                    "--emailType": email_type,
                    "--CONFIG_BUCKET": config_bucket,
                    "--CONFIG_KEY": config_key,
                    "--GLOBAL_CONFIG_KEY": global_config_key,
                    "--SNS_TOPIC_ARN": SNS_TOPIC_ARN,
                    "--REDIRECT_BASE_URL": REDIRECT_BASE_URL,
                    "--SHORT_LINKS_TABLE": SHORT_LINKS_TABLE,
                    "--REGENERACION_SIN_DATOS_NUEVOS": (
                        "true" if new.get(
                            "regeneracionSinDatosNuevos", {}
                        ).get("BOOL", False) else "false"
                    )
                }
            )

            logger.info("GLUE JOB DISPARADO")
            logger.info(f"JobRunId: {response['JobRunId']}")

        except Exception as e:
            logger.error(
                f"Error disparando Glue Job: {str(e)}",
                exc_info=True
            )