import os

'''
Debug level can be one of :
{
    'CRITICAL',
    'ERROR',
    'WARNING',
    'INFO',
    'DEBUG',
    'NOTSET'
}

to user the logger , use the below types . (eg: logger.warning(msg)

1. critical
2. error
3. exception
4. warn
5. info
6. debug
7. log

Notes:
1.  If debug level is DEBUG , boto3 will be printing all over ...
    It is recommended to use info to trace your prints.
2.  currently , printing to a file is not supported . if you want to support it for
    local environment , simply add filename='file.txt' as parameter to basicConfig function

'''
import logging

DEBUG_LEVEL = os.getenv('DEBUG_LEVEL', "INFO")
ENABLE_BOTO_LOGGER = os.getenv('ENABLE_BOTO_LOGGER', "False")

if ENABLE_BOTO_LOGGER == "False":
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
    logging.getLogger("requests").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger("binaryornot").setLevel(logging.CRITICAL)
    logging.getLogger("chardet").setLevel(logging.CRITICAL)


    # @ TODO: we can agree not to use boto3 prints at all , currently  , I don't think it's correct. we'll use critical.
    # logging.getLogger('boto3').propagate = False
    # logging.getLogger('botocore').propagate = False

root = logging.getLogger()
# removing the existing handlers. by default , if the handlers exists , our configuration will be deleted.
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(level=DEBUG_LEVEL,
                    format='%(asctime)s\t| %(levelname)s\t| %(filename)-15s\t| %(funcName)-20s:%(lineno)d\t| %(message)s',
                    datefmt="%d-%m-%Y %H:%M:%S")


logger = logging
