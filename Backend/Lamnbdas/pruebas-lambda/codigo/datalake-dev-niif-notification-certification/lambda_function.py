import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")

GLUE_JOB_NAME = "datalake-dev-niif-notification-certification"


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

            #  Solo cuando está en PENDING
            if new_status != "PENDING":
                continue

            #  Evitar reprocesar
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

            logger.info(
                f"Disparando Glue Job para useCaseId={use_case_id}, certCode={cert_code}"
            )

            response = glue.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    "--useCaseId": use_case_id,
                    "--certCode": cert_code,
                    "--emailType": email_type
                }
            )

            logger.info("GLUE JOB DISPARADO")
            logger.info(f"JobRunId: {response['JobRunId']}")

        except Exception as e:
            logger.error(
                f"Error disparando Glue Job: {str(e)}",
                exc_info=True
            )
