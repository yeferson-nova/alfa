import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # TODO implement
    session = boto3.session.Session()
    glue_client = session.client('glue')
    database_name = 'alfa-datalake-dev-gold'

    response = glue_client.get_tables(DatabaseName= database_name)

    tables = []
    
    try:
        for table in response['TableList']:
            response = glue_client.delete_table(DatabaseName= database_name, Name = table['Name'])
            tables.append(table)
            print(f"Tabla eliminada {table['Name']}")
    except ClientError as e:
        raise Exception( "boto3 client error in delete_a_table_from_database: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in delete_a_table_from_database: " + e.__str__())