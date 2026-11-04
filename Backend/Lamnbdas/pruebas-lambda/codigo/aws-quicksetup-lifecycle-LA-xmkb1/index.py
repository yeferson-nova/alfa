import boto3
import json
import urllib3
import os
import time

# CONSTANTS
SUCCESS = 'SUCCESS'
FAILED = 'FAILED'
# Events send in by CloudFormation
CREATE = 'Create'
UPDATE = 'Update'
DELETE = 'Delete'

# ENVIRONMENT VARIABLES
region = os.environ['REGION']

SECONDS_FOR_POLLING_COMPLETION = 840 # 14 mins max.

# CLIENTS
ssm_client = boto3.client('ssm', region_name= region)
http = urllib3.PoolManager()

def send_response(event, context, status, message=None):
    try:
        stack_id = check_for_key(event, 'StackId')
        request_id = check_for_key(event, 'RequestId')
        logical_resource_id = check_for_key(event, 'LogicalResourceId')
        resource_properties = event['ResourceProperties']
        physicalResourceId = resource_properties['AutomationParameters']['QSConfigurationId'][0] + '_' + resource_properties.get('QSType', 'TA')

        body = json.dumps({
            'Status': status,
            'Reason': message or f'For more details see SSM Automation execution for AWSQuickSetupType-SSM-ManageResources Document',
            'StackId': stack_id,
            'RequestId': request_id,
            'LogicalResourceId': logical_resource_id,
            'PhysicalResourceId': physicalResourceId})

        headers = {
            'content-type': '',
            'content-length': str(len(body))
        }

        print('Sending response to CloudFormation via http request...')
        responseUrl = event['ResponseURL']
        response = http.request(
            'PUT', responseUrl, headers=headers, body=body, retries=5)
        print('Status code:', response.status)
        if str(response.status) != '200':
            raise Exception(f'Received a failed response from Cloud Formation: {str(response.status)}')
    except Exception as ex:
        raise Exception(f'An Exception occurred while sending a response for Cloud Formation. {str(ex)}')

def check_for_key(hash, key):
    if not key in hash:
        raise Exception(f'The required key {key} is not present in {str(hash)}. Failing.')
    else:
        # return the value
        return hash[key]

def reconcile(event, context):
    request_type = event.get('RequestType')
    resource_properties = event['ResourceProperties']
    automation_document = resource_properties.get('AutomationDocument')
    automation_assume_role = resource_properties.get('AutomationAssumeRole')

    print('Event:', event)

    try:
        input_parameters = resource_properties['AutomationParameters']
        input_parameters['ExecutionMode'] = [request_type]
        if 'SkipManageInstanceRoleCreation' not in input_parameters and 'SkipDHMCRoleCreation' not in input_parameters and request_type == 'Delete':
            send_response(event, context, SUCCESS)
            return

        response = ssm_client.start_automation_execution(
            Tags=[{'Key': 'QuickSetupDocument', 'Value': 'AWSQuickSetupType-SSM'}],
            DocumentName=automation_document,
            Parameters=input_parameters,
            Mode='Auto'
        )
        print('Lifecycle automation execution started successfully.')
        execution_id = response['AutomationExecutionId']
        print('Lifecycle automation execution ID:', execution_id)
        execution_start_time = time.time()

        # Wait for the execution to complete. timeout for the execution is 10 minutes
        while time.time() - execution_start_time < SECONDS_FOR_POLLING_COMPLETION:
            automation_execution = ssm_client.get_automation_execution(
                AutomationExecutionId=execution_id
            )
            execution_status = automation_execution['AutomationExecution']['AutomationExecutionStatus']
            print('Lifecycle automation execution status:', execution_status)
            if execution_status in ['Success', 'Failed', 'Cancelled', 'Rejected', 'TimedOut', 'ChangeCalendarOverrideRejected', 'CompletedWithSuccess', 'CompletedWithFailure']:
                break
            # Check the status every 5 seconds
            time.sleep(10)

        if execution_status != 'Success' and execution_status != 'CompletedWithSuccess':
            print('Lifecycle failed due to automation status:', execution_status)
            send_response(event, context, FAILED)
            return

        print('Lifecycle succeeded.')
        send_response(event, context, SUCCESS)
    except Exception as err:
        print(err)
        print('Lifecycle failed, you can review the log for the Lambda function for details')
        send_response(event, context, FAILED, message=str(err))
        raise err
