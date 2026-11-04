import json
import boto3
from botocore.exceptions import ClientError
from src.utils.decorators.exception_handler import handle_exception


class SMHelper():
    
    def __init__(self):
        self._sm_client = boto3.client("secretsmanager")

    @handle_exception("request_param_value", "step_request_params")
    def request_param_value(
        self, 
        sm_id
        ):
        """
        Retrieves a secret's parameter values from a secret management client.

        Args:
            sm_id (str): The unique identifier of the secret to retrieve.

        Returns:
            dict: A dictionary containing the secret's parameter values.

        Raises:
            ClientError: If an error occurs while retrieving the secret.
        """

        try:
            print(self._sm_client)
            response = self._sm_client.get_secret_value(
                SecretId=sm_id
            )
            
            response = response['SecretString']
            response = json.loads(response)
            
            return response
            
        except Exception as _:
            raise ClientError({"Exception": {"Code": "400", "Message": "Client exception"}}, "request_param_value")
        
    @handle_exception("get_params_values", "step_request_params")
    def get_params_values(
        self, 
        sm_id:str
        ):
        """
        Retrieves parameter values associated with a specific identifier.

        Args:
            sm_id (str): A unique identifier used to fetch the parameters.

        Returns:
            dict: A dictionary containing the retrieved parameter values.
        """
        params = self.request_param_value(sm_id)
        return params