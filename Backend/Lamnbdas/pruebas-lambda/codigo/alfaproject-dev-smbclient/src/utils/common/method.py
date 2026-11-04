import os
import re
import smbclient
from collections import defaultdict
from datetime import datetime


def get_current_date_bucketkey():
    """
    Generates a string representing the current date in a bucket key format.
    The format of the returned string is `YYYY-M-D/`.

    Returns:
        str: The current date formatted as `YYYY-M-D/`.
    """
    return datetime.now().strftime("%Y%-m%-d") + "/" 

def get_lastest_filename(
    smb_files:list,
    ):
    """
    Identifies the latest file from a list of SMB file names based on versioning.

    The function groups files by their base name (before the extension) and selects 
    the file with the highest numeric version for each group. Version numbers are 
    extracted using a regular expression.

    Args:
        smb_files (list): A list of file names to process.

    Returns:
        list: A list of the latest file names, one for each group.

    Raises:
        ValueError: If a file name does not contain a numeric version.
    """
    
    if len(smb_files) == 0:
        return []

    grouped_files = defaultdict(list)
    
    for file in smb_files:

        full_filename = file.split(".")[0]
        date = re.search(r"\d+", full_filename).group()
        clean_name = full_filename.replace(
            f"_{date}", ""
        )
        
        grouped_files[clean_name].append(file)

    print(grouped_files)

    lastest_files = [
        max(group, key = lambda x: int( re.search(r"(\d+)(?=\.\w+$)", x).group() ) )
        for group in grouped_files.values()
    ]

    return lastest_files

def get_files_from_smb(
    path_location:str,
    ):
    """
    Retrieves a list of files from a specified SMB path.

    This function uses the `smbclient` library to list all files in the given 
    SMB directory.

    Args:
        path_location (str): The SMB directory path from which to retrieve the files.

    Returns:
        list: A list of file names in the specified SMB directory.

    Raises:
        SMBException: If an error occurs while accessing the SMB path.
    """
    
    files = smbclient.listdir(path_location, search_pattern=r"*")
    return files

def read_file_from_smb(
    path_location:str,
    filename:str,
    ):
    """
    Reads the contents of a file from a specified SMB path.

    This function uses the `smbclient` library to open and read a file in binary mode 
    from the given SMB directory.

    Args:
        path_location (str): The SMB directory path where the file is located.
        filename (str): The name of the file to read.

    Returns:
        bytes: The contents of the file in binary format.

    Raises:
        SMBException: If an error occurs while accessing the SMB path or file.
        FileNotFoundError: If the specified file does not exist in the given path.
    """
    
    with smbclient.open_file(os.path.join(path_location, filename), mode='rb') as file:
        file_data = file.read()
        
    return file_data