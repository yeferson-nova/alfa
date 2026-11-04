import json
import boto3
import csv
import io
import os
import traceback
import uuid
import pandas as pd

from datetime import datetime

s3_client = boto3.client('s3')
sniffer = csv.Sniffer()

bucket_name = os.environ['BUCKET_NAME']
files_path = os.environ['FILES_PATH']

id_proccess = uuid.uuid4()

today_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
today_file = datetime.now().strftime("%Y-%m-%d")


def create_empty_df():
    columnas = ["pk_id", "file", "expected_fields","identify_fields", "ophans_fields", "pk_key", "%_fields", "date"]
    df = pd.DataFrame(columns=columnas)

    return df

def load_json_config(filename):
    """Load JSON configuration file"""
    content = get_content(bucket_name, "calidad/Config/rule_files.json")
    content = content.decode('utf-8')
    try:
        return json.loads(content)
        
    except Exception as e:
        print(f"Error loading {filename}: {str(e)}")
        return {}

def get_content (bucket_name, file_key):
    """Get the content of a file from S3"""
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    content = response['Body'].read()
    return content

def validate_content(file_name, headers_file, rules):
    return False

def get_first_row_from_csv(csv_content, delimiter:str = '|'):
    """Get the first row of a CSV file"""
    # Create a StringIO object from the CSV content
    csv_file = io.StringIO(csv_content)
    
    # Create a CSV reader object
    csv_reader = csv.reader(csv_file)
    
    # Read the first row
    first_row = next(csv_reader)
    
    return first_row

def get_first_row_from_txt_file(txt_content):
    "test"
    return False


def lambda_handler(event, context):
    # TODO implement

    config = load_json_config('s3://alfa-dev-cl-bronz-jl/calidad/Config/rule_files.json')

    df_table = create_empty_df()

    df_data = []

    if not bucket_name:
        return {
            'statusCode': 500,
            'body': json.dumps('BUCKET_NAME environment variable not set')
        }
    
    files_found = []

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=files_path)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                file_key = obj['Key']
                if not file_key.endswith('/'):
                    files_found.append(file_key)

        for file in files_found:

            if file.endswith('.csv'):
                print("entre a un csv")
            elif file.endswith('.txt'):
                
                content = get_content(bucket_name, file_key=file)
                content = content.decode('utf-8')
                dialact = sniffer.sniff(content[:1024])
                reader = csv.reader(io.StringIO(content), delimiter=dialact.delimiter)
                next_r = next(reader)
                for data, conf in config.items():
                    file_name = file.split("/")[-1]
                    if data == file_name.split(".")[0]:
                        print(f"NOMBRE ARCHIVO: {file_name}")
                        if len(next_r) != conf["num_count"]:
                            print(f"no coinciden los campos, campos esperados {len(next_r)}, campos optenidos {conf["num_count"]}")
                        else:
                            print(f"los campos coinciden campos esperados {len(next_r)}, campos optenidos {conf["num_count"]}")
                        count = 0
                        orphan_fields = []
                        for field in conf["necesary_fields"]:
                            if field.upper() not in [x.upper() for x in next_r]: 
                                orphan_fields.append(field) 
                                print(f"el campo {field} no se encuentra en el archivo")
                            else: count+= 1
                        print(f"campos encontrados {count}")
                        print(f"el % de campos encontrados es de {count*100/len(next_r)}%")
                        if conf["file_pk"].upper() not in [x.upper() for x in next_r]: print(f"la llave primaria {conf["file_pk"]} no se encuentra en el archivo")
                        else: print(f"la llave primaria {conf["file_pk"]} se encuentra en el archivo")

                        df_data.append({"pk_id": str(id_proccess), "file": file_name, "expected_fields": len(next_r), "identify_fields": count, "ophans_fields": orphan_fields, "pk_key": conf["file_pk"], "%_fields": count*100/len(next_r), "date": today_date})
        

            elif file.endswith('.xlsx'):
                content = get_content(bucket_name, file_key=file)
                df = pd.read_excel(
                        io.BytesIO(content), 
                        skiprows = 1,
                        nrows = 1,
                        header = None,
                        engine='openpyxl'
                    )
                
                if df.empty:
                    print("file is empty")

                for data, conf in config.items():
                    file_name = file.split("/")[-1]
                    if data == file_name.split(".")[0]:
                        print(f"NOMBRE ARCHIVO: {file_name}")
                        if len(df.iloc[0].tolist()) != conf["num_count"]:
                            print(f"no coinciden los campos, campos esperados {len(df.iloc[0].tolist())}, campos optenidos {conf["num_count"]}")
                        else:
                            print(f"los campos coinciden campos esperados {len(df.iloc[0].tolist())}, campos optenidos {conf["num_count"]}")
                        count = 0
                        orphan_fields = []
                        for field in conf["necesary_fields"]:
                            if field.upper() not in [x.upper() for x in df.iloc[0].tolist()]:
                                orphan_fields.append(field) 
                                print(f"el campo {field} no se encuentra en el archivo")
                            
                            else: count+= 1
                        print(f"campos encontrados {count}")
                        print(f"el % de campos encontrados es de {count*100/len(df.iloc[0].tolist())}%")
                        if conf["file_pk"].upper() not in [x.upper() for x in df.iloc[0].tolist()]: print(f"la llave primaria {conf["file_pk"]} no se encuentra en el archivo xlsx")
                        else: print(f"la llave primaria {conf["file_pk"]} se encuentra en el archivo xlsx")

                        df_data.append({"pk_id": str(id_proccess), "file": file_name, "expected_fields": len(df.iloc[0].tolist()), "identify_fields": count, "ophans_fields": orphan_fields, "pk_key": conf["file_pk"], "%_fields": count*100/len(df.iloc[0].tolist()), "date": today_date})
            else:
                print("File type not supported")

        df_final = pd.DataFrame(df_data)
        csv_buffer = io.StringIO()
        #f"s3://alfa-dev-cl-bronz-jl/calidad/Files_out/{id_proccess}.csv"
        df_final_csv = df_final.drop(columns=['pk_id'])
        df_final_csv.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key=f"calidad/Files_out/{today_file}/quality_file_{today_file}.csv", Body=csv_buffer.getvalue())

        parquet_buffer = io.BytesIO()
        df_final.to_parquet(parquet_buffer, index=False,engine="pyarrow",compression="snappy")
        parquet_buffer.getvalue()
        
        s3_client.put_object(Bucket=bucket_name, Key=f"calidad/tb_quality_files/{today_file}/quality_file_{today_file}.parquet", Body=parquet_buffer.getvalue())

        return {
            'statusCode': 200
        }

    except Exception as e:
        print("Error:", str(e))
        print(traceback.format_exc())

        return {
            'statusCode': 500,
            'body': json.dumps('Error processing request'),
            'error': str(e)
        }

