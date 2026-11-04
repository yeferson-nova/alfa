import os
import smbclient    
import tempfile
from dataclasses import dataclass
from src.aws.s3_helper import S3Helper

@dataclass
class SmbHelper:
    
    
    host:str
    user_name:str
    password:str
    port:str
    path:str
    
    def smb_register_session(
        self,
        ):
        
        smbclient.register_session(
            server=self.host, 
            username=self.user_name, 
            password=self.password,
        )
        
    def smb_read_and_upload_file(
        self,
        environment_vars:dict,
        smb_filename:str,
        path_location:str,
        obj_s3helper:S3Helper,
        ):
        
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        
            with smbclient.open_file(os.path.join(path_location, smb_filename), mode='rb') as file:
                file_data = file.read()
                temp_file.write(file_data)
                temp_file.flush()
        
            _ = obj_s3helper.upload_object(
                bucket_name=environment_vars["BUCKET_NAME"],
                bucket_key=environment_vars["BUCKET_KEY"] + environment_vars["OUTPUT_FILENAME"],
                file_name=temp_file.name,
            )

            print("Cambiando el archivo de lugar")
            smbclient.rename(
                f"{path_location}/{smb_filename}",
                f"{path_location}/PROCCESS_FILE/{smb_filename}"
            )
            
            del file_data