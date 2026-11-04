import time
import pandas as pd
import json
import io
import boto3
from src.utils.handler.logger_base import log
from src.utils.decorators.exception_handler import handle_exception
from botocore.exceptions import ClientError


class S3Helper():
    
    
    def __init__(self):
        self._s3_client = boto3.client("s3")
        self._s3_resource  = boto3.resource("s3")    

    def get_if_exist_object(
        self,
        bucket_name:str, 
        bucket_key:str
        ):
        """
        Checks if an object exists in an S3 bucket.

        This method uses the AWS S3 `head_object` API to verify if a specified object exists
        in a given bucket. If the object exists, it returns a list containing a dictionary
        with the object's key. If the object does not exist or an error occurs, it returns an empty list.

        Args:
            bucket_name (str): The name of the S3 bucket.
            bucket_key (str): The key (path) of the object in the S3 bucket.
        
        Returns:
            list: A list containing a dictionary with the object's key if it exists, otherwise an empty list.
        
        Example:
            >>> result = self.get_if_exist_object("my-bucket", "path/to/file.txt")
            >>> print(result)
            [{"Key": "path/to/file.txt"}]  # If the file exists
            >>> result = self.get_if_exist_object("my-bucket", "path/to/missing_file.txt")
            >>> print(result)
            []  # If the file does not exist
        
        Logging:
        - Logs the bucket name and key being checked.
        """
        
        try:
            
            log.info(
                f"File validate if exist - Bucket: {bucket_name}, Bucket_key: {bucket_key}"
            )
            
            self._s3_client.head_object(
                Bucket=bucket_name,
                Key=bucket_key,
            )
            
            return [{
                "Key": bucket_key,
            }]
    
        except Exception as _:
            return []
    
    @handle_exception("get_object_from_json", "step_extract_file")
    def get_object_from_json(
        self, 
        bucket_name:str, 
        bucket_key:str
        ):
        """
        Fetches and parses a JSON object from an S3 bucket.

        This method retrieves a JSON file stored in the specified S3 bucket and parses its contents
        into a Python dictionary. If the file does not exist or another error occurs, it raises a
        `ClientError` with a descriptive message.

        Args:
            bucket_name (str): The name of the S3 bucket.
            bucket_key (str): The key (path) of the JSON file in the S3 bucket.
        
        Returns:
            dict: A dictionary representing the parsed JSON content of the file.
        
        Raises:
            ClientError: If the object does not exist in the bucket or another error occurs.
        
        Example:
            >>> json_data = self.get_object_from_json("my-bucket", "path/to/file.json")
            >>> print(json_data)
            {'key1': 'value1', 'key2': 'value2'}
        
        Logging:
            - Logs the bucket name and key being processed.
        
        Error Handling:
            - Raises a `ClientError` if the file is missing or unreadable.
        """
    
        try:
            
            log.info(
                f"File to extract - Bucket: {bucket_name}, Bucket_key: {bucket_key}"
            )
            
            response = self._s3_client.get_object(
                Bucket=bucket_name,
                Key=bucket_key
            )
            
            json_data = response["Body"].read()
            json_data = json.loads(json_data)
            
            return json_data
    
        except ClientError as e:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": f"ERROR {e}, proceessing Bucket: {bucket_name}, Object: {bucket_key}"}}, "get_object")
        
    
    @handle_exception("get_object_from_xlsx", "step_extract_file", ClientError)
    def get_object_from_xlsx(
        self, 
        bucket_name:str,
        bucket_key:str,
        sheet_name:str=0,
        skiprows:str=None,
        usecols:list=None
        ):
        """
        Fetches and parses an Excel file from an S3 bucket into a Pandas DataFrame.

        This method retrieves an Excel file stored in the specified S3 bucket and loads its content
        into a Pandas DataFrame. The method supports optional parameters for sheet selection,
        skipping rows, and selecting specific columns.

        Args:
            bucket_name (str): The name of the S3 bucket.
            bucket_key (str): The key (path) of the Excel file in the S3 bucket.
            sheet_name (str, int, or list, optional): The name, index, or list of sheet names to load. Default is the first sheet (0).
            skiprows (int, list-like, or callable, optional): Rows to skip at the beginning (0-indexed).
            usecols (list, optional): Columns to parse from the Excel file.
        
        Returns:
            pd.DataFrame: A Pandas DataFrame containing the parsed content of the Excel file.
        
        Raises:
            ClientError: If the object does not exist in the bucket or another error occurs.
        
        Example:
            >>> df = self.get_object_from_xlsx(
            ...     bucket_name="my-bucket",
            ...     bucket_key="path/to/file.xlsx",
            ...     sheet_name="Sheet1",
            ...     skiprows=2,
            ...     usecols=["A", "B", "C"]
            ... )
            >>> print(df)
        
        Logging:
            - Logs the bucket name and key being processed.
        
        Error Handling:
            - Raises a `ClientError` with a detailed message if the file is missing or unreadable.
        """
        
        try:
            
            log.info(
                f"File to extract - Bucket: {bucket_name}, Bucket_key: {bucket_key}"
            )
            
            response = self._s3_client.get_object(
                Bucket=bucket_name,
                Key=bucket_key
            )
            
            excel_content = response["Body"].read()
                        
            df_data = pd.read_excel(
                io.BytesIO(excel_content),
                sheet_name = sheet_name, 
                skiprows = skiprows,
                usecols=usecols,
                engine="openpyxl"
            )
            
            return df_data
    
        except ClientError as e:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": f"ERROR {e}, proceessing Bucket: {bucket_name}, Object: {bucket_key}"}}, "get_object")


    @handle_exception("get_object_from_csv", "step_extract_file", ClientError)
    def get_object_from_csv(
        self, 
        bucket_name:str, 
        bucket_key:str, 
        sep:str, 
        header:str, 
        skiprows:str, 
        encoding:str
        ):
        """
        Fetches and parses a CSV file from an S3 bucket into a Pandas DataFrame.

        This method retrieves a CSV file stored in the specified S3 bucket and loads its content
        into a Pandas DataFrame. The method supports optional parameters for delimiter, header row,
        rows to skip, and encoding.

        Args:
            bucket_name (str): The name of the S3 bucket.
            bucket_key (str): The key (path) of the CSV file in the S3 bucket.
            sep (str): The delimiter to use for parsing the CSV file.
            header (int or None): Row number to use as the column names, or `None` if the file has no header.
            skiprows (int or list-like): Rows to skip at the beginning of the file.
            encoding (str): The encoding of the CSV file.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the parsed content of the CSV file.

        Raises:
            ClientError: If the object does not exist in the bucket or another error occurs.

        Example:
            >>> df = self.get_object_from_csv(
            ...     bucket_name="my-bucket",
            ...     bucket_key="path/to/file.csv",
            ...     sep=",",
            ...     header=0,
            ...     skiprows=2,
            ...     encoding="utf-8"
            ... )
            >>> print(df)

        Logging:
            - Logs the bucket name and key being processed.

        Error Handling:
            - Raises a `ClientError` with a detailed message if the file is missing or unreadable.
        """
        
        try:
            
            log.info(
                f"File to extract - Bucket: {bucket_name}, Bucket_key: {bucket_key}"
            )
            
            response = self._s3_client.get_object(
                Bucket = bucket_name,
                Key = bucket_key
            )
            
            csv_content = response["Body"].read().decode(encoding)
            df_data = pd.read_csv(
                io.StringIO(csv_content), 
                sep = sep,
                header = header,
                skiprows=skiprows,
                engine = "python"
            )
            
            return df_data
    
        except ClientError as e:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": f"ERROR {e}, proceessing Bucket: {bucket_name}, Object: {bucket_key}"}}, "get_object")
    
    @handle_exception("upload_object", "step_upload_file")
    def upload_object(
        self, 
        bucket_name:str, 
        bucket_key:str, 
        file_name:str
        ):
        """
        Uploads a local file to an S3 bucket.

        This method uploads a file from the local file system to a specified S3 bucket and key (path).
        It logs the operation and raises an exception if the upload fails.

        Args:
            bucket_name (str): The name of the S3 bucket.
            bucket_key (str): The key (path) in the S3 bucket where the file will be uploaded.
            file_name (str): The local path of the file to upload.

        Returns:
            None: The response from the `upload_file` method is not captured since it only raises exceptions on failure.

        Raises:
            Exception: If the upload fails for any reason.

        Example:
            >>> self.upload_object(
            ...     bucket_name="my-bucket",
            ...     bucket_key="path/to/uploaded_file.txt",
            ...     file_name="local_file.txt"
            ... )

        Logging:
            - Logs the bucket name and key being processed.

        Error Handling:
            - Raises a general exception with details about the file and bucket if the upload fails.
        """
                
        try:
            
            log.info(
                f"File to upload - Bucket: {bucket_name}, Bucket_key: {bucket_key}"
            )
            
            response = self._s3_client.upload_file(
                Filename=file_name, 
                Bucket=bucket_name, 
                Key=bucket_key
            )
            
            return response
        
        except Exception as e:
            raise ClientError({"Error": {"Code": "Exception", "Message": f"error {e}, proceessing Bucket: {bucket_name}, Object: {bucket_key}"}}, "upload_object")
             
    @handle_exception("put_object", "step_put_object")
    def put_object(
        self, 
        bucket_name:str, 
        bucket_key:str, 
        file_content:str
        ):
        """
        Uploads a local file to an S3 bucket.

        This method uploads a file from the local file system to a specified S3 bucket and key (path).
        It logs the operation and raises an exception if the upload fails.

        Args:
            bucket_name (str): The name of the S3 bucket.
            bucket_key (str): The key (path) in the S3 bucket where the file will be uploaded.
            file_content (str|byte): The local path of the file to upload, or the content file in bytes.

        Returns:
            None: The response from the `upload_file` method is not captured since it only raises exceptions on failure.

        Raises:
            Exception: If the upload fails for any reason.

        Example:
            >>> self.upload_object(
            ...     bucket_name="my-bucket",
            ...     bucket_key="path/to/uploaded_file.txt",
            ...     file_name="local_file.txt"
            ... )

        Logging:
            - Logs the bucket name and key being processed.

        Error Handling:
            - Raises a general exception with details about the file and bucket if the upload fails.
        """
                
        try:
            
            log.info(
                f"File to upload - Bucket: {bucket_name}, Bucket_key: {bucket_key}"
            )
            
            response = self._s3_client.put_object(
                Body=file_content, 
                Bucket=bucket_name, 
                Key=bucket_key
            )
            
            return response
            
        except Exception as e:
            raise ClientError({"Error": {"Code": "Exception", "Message": f"error {e}, proceessing Bucket: {bucket_name}, Object: {bucket_key}"}}, "put_object")
                
    @handle_exception("list_objects", "step_list_objects")
    def list_objects(
        self, 
        bucket_name:str, 
        bucket_key:str
        ):
        """
        Lists objects in an S3 bucket under a specified prefix.

        This method retrieves all objects in a specified S3 bucket that match a given prefix.
        It returns a list of dictionaries containing the keys of the objects found, excluding empty objects.

        Args:
            bucket_name (str): The name of the S3 bucket.
            bucket_key (str): The prefix (path) in the bucket to list objects from.

        Returns:
            list: A list of dictionaries containing the keys of objects. 
                Returns an empty list if no objects are found.

        Raises:
            Exception: If an error occurs during the operation.

        Example:
            >>> objects = self.list_objects(
            ...     bucket_name="my-bucket",
            ...     bucket_key="path/to/"
            ... )
            >>> print(objects)
            [{'Key': 'path/to/file1.txt'}, {'Key': 'path/to/file2.txt'}]

        Logging:
            - Logs the bucket name and prefix being listed.

        Error Handling:
            - Raises a general exception with details about the bucket and prefix if the operation fails.
        """
        
        try:
            
            log.info(
                f"Bucket to list - Bucket: {bucket_name}, Bucket_key: {bucket_key}"
            )
            
            response = self._s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=bucket_key,
            )
            
            response = response.get("Contents", [])
            
            if response == []:
                return []
            
            else:
                objects = [ {"Key": item["Key"]} for item in response if item["Size"] > 0]
                return objects
            
            
        except Exception as e:
            raise ClientError({"Error": {"Code": "Exception", "Message": f"error {e}, proceessing Bucket: {bucket_name}, Object: {bucket_key}"}}, "list_objects_v2")
    
    
    @handle_exception("delete_objects", "step_delete_objects")
    def delete_objects(
        self, 
        bucket_name:str, 
        objects: list
        ):
        """
        Deletes multiple objects from an S3 bucket.

        This method deletes the specified objects from the given S3 bucket. The method logs the operation and
        raises an exception if the deletion fails.

        Args:
            bucket_name (str): The name of the S3 bucket.
            objects (list): A list of dictionaries representing the objects to delete. Each dictionary must contain the key `"Key"` 
            specifying the object's key.

        Returns:
            dict: A dictionary containing the response from the S3 `delete_objects` API.

        Raises:
            Exception: If the deletion operation fails.

        Example:
            >>> response = self.delete_objects(
            ...     bucket_name="my-bucket",
            ...     objects=[{"Key": "path/to/file1.txt"}, 
            {"Key": "path/to/file2.txt"}]
            ... )
            >>> print(response)

        Logging:
            - Logs the bucket name and the objects to be deleted.

        Error Handling:
            - Raises a general exception with details about the objects if the operation fails.
        """
    
        try:
            
            if objects is None or objects == []:
                return {
                    "response": "No data to delete"
                }
            
            log.info(
                f"Files to delete - Bucket: {bucket_name}, Objects: {objects}"
            )
            
            response = self._s3_client.delete_objects(
                Bucket = bucket_name,
                Delete = {
                    "Objects": objects
                }
            )
            
            time.sleep(5)
            
            return response
        
        except Exception as e:
            raise ClientError({"Error": {"Code": "Exception", "Message": f"error {e}, proceessing Bucket: {bucket_name}"}}, "delete_objects")