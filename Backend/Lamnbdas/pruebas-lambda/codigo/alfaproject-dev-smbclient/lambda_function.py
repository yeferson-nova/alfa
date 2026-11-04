import os
import time
import traceback
import src.utils.common.method as mtd
from src.classes.http_helper import HttpHelper
from smbprotocol.exceptions import SMBOSError
from src.classes.smb_helper import SmbHelper
from src.aws.sm_helper import SMHelper
from src.aws.s3_helper import S3Helper
from src.utils.handler.logger_base import log


def valid_filename(
    list_files:list,
    filename:str
    ) -> bool:

    for file in list_files:
        if filename.startswith(file):
            return True
    
    return False

def get_files_to_extract(
    list_files:list,
    path_location:str,
    ):

    smb_files = mtd.get_files_from_smb(
        path_location=path_location,
    )
    
    smb_files = [
        file
        for file in smb_files if valid_filename(list_files, file)
    ]

    return smb_files

def get_objects_from_server(
    environment_vars:dict,
    obj_smb:SmbHelper,
    obj_s3helper:S3Helper,
    ) -> list:

    for path_location in environment_vars["LOCAL_PATH"]:

        smb_files = get_files_to_extract(
            list_files=environment_vars["LOCAL_SMB_FILES"],
            path_location=os.path.join(obj_smb.path, path_location),
        )
        log.info(smb_files)
        
        smb_files = mtd.get_lastest_filename(
            smb_files=smb_files,
        )
        log.info(smb_files)

        for smb_file in smb_files:
            
            obj_smb.smb_read_and_upload_file(
                environment_vars=environment_vars,
                obj_s3helper=obj_s3helper,
                path_location=os.path.join(obj_smb.path, path_location),
                smb_filename=smb_file,
            )

def lambda_handler(
    event, 
    _
    ):
    
    try: 
        bucket_date = mtd.get_current_date_bucketkey()
        outputname, _ = event.get("output_name", "").split(".")
            
        environment_vars = {
            "LOCAL_PATH"        : event.get("path", "").split(","),
            "LOCAL_SMB_FILES"   : event.get("file_name", "").split(","),
            "OUTPUT_FILENAME"   : event.get("output_name", ""),
            "BUCKET_NAME"       : os.getenv("BUCKET_NAME", ""),
            "BUCKET_KEY"        : os.getenv("BUCKET_PATH", "") + f"{bucket_date}{outputname}/",
            "SECRET_NAME"       : os.getenv("SECRET_NAME", ""),
        }
        
        obj_s3helper = S3Helper()
        obj_smhelper = SMHelper()
        sm_values = obj_smhelper.get_params_values(
            sm_id=environment_vars["SECRET_NAME"],
        )

        obj_smb = SmbHelper(
            host=sm_values["HOST"],
            user_name=sm_values["USERNAME"],
            password=sm_values["PASSWORD"],
            port=sm_values["PORT"],
            path=sm_values["PATH"],
        )
        obj_smb.smb_register_session()
        
        get_objects_from_server(
            environment_vars=environment_vars,
            obj_smb=obj_smb,
            obj_s3helper=obj_s3helper,
        )
        
        return HttpHelper.execution_successfull(
            details="No warnbing details"
        )
    
    except SMBOSError as _:
        
        time.sleep(5)
        response = obj_s3helper.get_if_exist_object(
            bucket_name=environment_vars["BUCKET_NAME"],
            bucket_key=environment_vars["BUCKET_KEY"] + environment_vars["OUTPUT_FILENAME"],
        )
        
        if response == []:
            return HttpHelper.execution_failed(
                details=traceback.format_exc()[:10000]
            )
        
        else: 
            return HttpHelper.execution_successfull(
                details=traceback.format_exc()[:10000]
            )
        
    except Exception as _:

        return HttpHelper.execution_failed(
            details=traceback.format_exc()[:10000]
        )