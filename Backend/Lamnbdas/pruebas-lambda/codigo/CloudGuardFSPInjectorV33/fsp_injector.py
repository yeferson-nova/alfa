"""
this function Adds/Removes FSP
"""

import json
import os
import platform
import re
import secrets
import shutil
import time
import traceback
import zipfile

import boto3
import wget
from exceptions import (FspAccessDeniedError, FspCodeSizeIsTooLargeError,
                        FspCombinedCodeLayerSizeIsTooLargeError,
                        FspInjectorException, FspInjectorUnexpectedException,
                        FspMaxLayerSizeError, FspUpdateFunctionConfigError,
                        FspUpdateFunctionNotEnoughMemoryError, ProtegoError,
                        errors_mapping)
from p_logger import logger
from cg_request import CGRequest
from cg_response import CGResponse

QUALIFIER = "$LATEST"
MILLISECONDS_IN_MONTH = 60 * 60 * 1000 * 24 * 30
MAX_NUM_OF_CW_RETRIES = 5
MIN_MEMORY_MB_REQUIRE = 20
MAX_LAYER_SIZE = 5
JAVA8_AGENT = "/opt/java/lib/cloudguard_java8_agent-1.0.jar"
JAVA11_AGENT = "/opt/java/lib/cloudguard_java11_agent-1.0.jar"
NODE_PACKAGE_NAME = "@protego/protego-node-agent"
NODE_OPTIONS_ENV_VAR_VALUE = "--require @protego/protego-node-agent"
NODE_OPTIONS = "NODE_OPTIONS"
JAVA8_TOOL_OPTIONS_ENV_VAR_VALUE = "-javaagent:/opt/java/lib/cloudguard_java8_agent-1.0.jar"
JAVA11_TOOL_OPTIONS_ENV_VAR_VALUE = "-javaagent:/opt/java/lib/cloudguard_java11_agent-1.0.jar -javaagent:/opt/java/lib/cloudguard_java8_agent-1.0.jar"
JAVA_TOOL_OPTIONS = "JAVA_TOOL_OPTIONS"
DOTNET_STARTUP_HOOKS_VAL = "/opt/Cloudguard/StartupHook.dll"
DOTNET_STARTUP_HOOKS = "DOTNET_STARTUP_HOOKS"
DOTNET_STARTUP_HOOKS_UNIX_SEPARATOR = ":"
DOTNET_STARTUP_HOOKS_WINDOWS_SEPARATOR = ";"
LATEST_MANIFEST_FILE_PATH = "/var/task/bundle/latest/manifest.json"
LATEST_FSP_VERSION = None

PROTEGO_DIR_DEFAULT = "/var/task/bundle/latest"
PROTEGO_FSP_WORKING_DIR = "/tmp/.protego/.cache"
PROTEGO_FSP_DIR_TMP = "/tmp/.protego/.cache/fsp"
FSP_VERSIONS_DIR_EXTRACTED_PATH = "/tmp/fsp_versions"
CURRENT_FSP_VERSION = None
fsp_versions = []


def lambda_handler(event, context):
    logger.info("Input event - %s", event)
    # the version retrieved from the manifest file, bundled with this function is set as the latest fsp version.
    global LATEST_FSP_VERSION
    LATEST_FSP_VERSION = json.load(open(LATEST_MANIFEST_FILE_PATH))["version"]
    configure_bundle()

    fsp_action = event.get("Action", None)
    function_arn = event.get("FunctionArn", None)
    fsp_version = event.get("FspVersion", None)
    fsp_bundle_url = event.get("FspBundleURL", None)

    request = CGRequest()
    if not function_arn or not fsp_action:
        raise Exception("FunctionArn or FSP Action is None")

    if fsp_action not in [CGRequest.Actions.Add, CGRequest.Actions.Remove]:
        raise Exception("Bad Input Action")

    request.action = fsp_action
    request.functionArn = function_arn

    if (fsp_version and fsp_bundle_url is None) or (fsp_version is None and fsp_bundle_url):
        raise Exception("Bad Input fspVersion or fspBundleURL")

    request.fspVersion = fsp_version
    request.fspBundleUrl = fsp_bundle_url
    region = get_region(request.functionArn)
    response = handle_fsp_action(request, region)

    logger.info("Response: %s", response.to_str())
    return response.to_json()

def create_json_error(exc, last_modified=None):
    message = None
    error_type = None
    error_id = None
    if isinstance(exc, FspInjectorUnexpectedException):
        raise FspInjectorUnexpectedException(exc)
    elif isinstance(exc, FspMaxLayerSizeError):
        error_type = "FspMaxLayerSizeError"
        error_id = errors_mapping[error_type]["id"]
        message = exc.message
    elif isinstance(exc, FspAccessDeniedError):
        error_type = "FspAccessDeniedError"
        error_id = errors_mapping[error_type]["id"]
        message = exc.message
    elif isinstance(exc, FspCombinedCodeLayerSizeIsTooLargeError):
        error_type = "FspCombinedCodeLayerSizeIsTooLargeError"
        error_id = errors_mapping[error_type]["id"]
        message = exc.message
    elif isinstance(exc, FspCodeSizeIsTooLargeError):
        error_type = "FspCodeSizeIsTooLargeError"
        error_id = errors_mapping[error_type]["id"]
        message = exc.message
    elif isinstance(exc, FspUpdateFunctionNotEnoughMemoryError):
        error_type = "FspUpdateFunctionNotEnoughMemoryError"
        message = exc.message
    elif isinstance(exc, FspUpdateFunctionConfigError):
        error_type = "FspUpdateFunctionConfigError"
        message = exc.message

    return json.dumps({
        "Message": message,
        "ErrorType": error_type,
        "Stack": traceback.format_exc(),
        "Id": error_id,
        "LastModified": last_modified
    })

def configure_bundle():
    latest_version_dir = FSP_VERSIONS_DIR_EXTRACTED_PATH + "/" + LATEST_FSP_VERSION
    if os.path.exists(latest_version_dir):
        logger.info("(Warm Start)")
    else:
        logger.debug("(Cold Start) Copy bundle to /tmp ...")
        try:
            shutil.copytree(PROTEGO_DIR_DEFAULT, latest_version_dir)
            fsp_versions.append(LATEST_FSP_VERSION)
            copy_to_fsp_version(latest_version_dir)
            global CURRENT_FSP_VERSION
            CURRENT_FSP_VERSION = LATEST_FSP_VERSION
            logger.info("(Cold Start) Done.")
        except OSError as ex:
            logger.error("Copy directory exception: %s", str(ex))


def get_region(function_arn):
    splited_function_arn = function_arn.split(':')
    return splited_function_arn[3]


def handle_fsp_action(request, region):
    if request.action == CGRequest.Actions.Add:
        try:
            get_fsp_versioned_bundle(request, CURRENT_FSP_VERSION)
        except Exception as exp_info:
            raise FspInjectorUnexpectedException(
                f"Unable to get fsp versioned bundle : {exp_info}") from exp_info
    function_configuration = {}
    try:
        # fetch function config
        function_configuration = fetch_function_config(
            request.functionArn, region)
        runtime = function_configuration.get("Configuration").get("Runtime")

        validate_function_configuraion(request.functionArn, request.action,
                            function_configuration, region)
        new_env_var, new_handler, new_layers = calculate_function_config(request.functionArn, request.action,
                                                                        function_configuration, region, runtime)
        res = update_function_config(
            request.functionArn, new_env_var, new_handler, new_layers, region)
    except Exception as exc:
        logger.info("FspInjectorException - %s", exc)
        json_error = create_json_error(exc, function_configuration.get("Configuration", {}).get("LastModified", None))
        raise FspInjectorException(json_error) from exc

    return CGResponse("Action Done successfully!", res.get("LastModified"), request)


def get_fsp_versioned_bundle(request, current_fsp_version):
    bundle_version = request.fspVersion if request.fspVersion else LATEST_FSP_VERSION
    if bundle_version == current_fsp_version:
        logger.info("bundle_version and current_fsp_version are equal, no need to download")
        return
    extract_path = FSP_VERSIONS_DIR_EXTRACTED_PATH + "/" + bundle_version

    # Current FSPInjector FSP version not equals to the current FSP version but was alredy in used (already downloaded and extracted)
    # needs to copy from the extracted path to /tmp/.protego/.cache/fsp in order to use.
    if bundle_version in fsp_versions:
        logger.info("bundle version already exist in fsp versions list")
        copy_to_fsp_version(extract_path)
        global CURRENT_FSP_VERSION
        CURRENT_FSP_VERSION = bundle_version
        return

    try:
        if not os.path.exists(extract_path):
            os.makedirs(extract_path)
    except Exception as exc_info:
        raise ProtegoError(f"{extract_path} could not be created: {exc_info}") from exc_info

    # Current FSPInjector FSP version not equals to the required FSP version and used for the first time -
    # needs to download from s3 (with the presignedURL), extract the zip file and copy to /tmp/.protego/.cache/fsp in order to use.
    # download bundle
    download_fsp_bundle(request.fspBundleUrl, extract_path)

    copy_to_fsp_version(extract_path)
    CURRENT_FSP_VERSION = bundle_version
    fsp_versions.append(bundle_version)

def copy_to_fsp_version(extract_path):
    try:
        if os.path.exists(PROTEGO_FSP_DIR_TMP):
            logger.info("Delete %s if it exists", PROTEGO_FSP_DIR_TMP)
            shutil.rmtree(PROTEGO_FSP_DIR_TMP)
        logger.info("Create %s", PROTEGO_FSP_DIR_TMP)
        shutil.copytree(extract_path, PROTEGO_FSP_DIR_TMP)
    except Exception as exc_info:
        raise ProtegoError(f"Error copying to tmp dir: {exc_info}") from exc_info


def download_fsp_bundle(bundle_url, extract_path):
    zip_file = 'bundle.zip'
    try:
        downloaded_file = os.path.join(extract_path, zip_file)
        wget.download(bundle_url, out=downloaded_file)

        # Check if the downloaded file is a ZIP file
        if zipfile.is_zipfile(downloaded_file):
            # Extract the contents of the ZIP file
            with zipfile.ZipFile(downloaded_file, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            logger.info(f"ZIP file downloaded and extracted to '{extract_path}'")
            # Delete the ZIP file after extraction
            os.remove(downloaded_file)
            logger.info(f"Deleted ZIP file: {downloaded_file}")
        else:
            raise ProtegoError("File downloaded is not a ZIP file")

    except zipfile.BadZipFile as ex:
        raise ProtegoError(f"Error occurred while extracting the ZIP file: {ex}") from ex

    except Exception as ex:
        raise ProtegoError(f"An unexpected error occurred: {ex}") from ex


def fetch_function_config(function_arn, region):
    lambda_client = boto3.client('lambda', region_name=region)
    waiter = lambda_client.get_waiter('function_updated')
    waiter.wait(FunctionName=function_arn)

    try:
        return lambda_client.get_function(
            FunctionName=function_arn,
            Qualifier=QUALIFIER
        )
    except Exception as exc_info:
        if exc_info == "Missing region in config":
            raise ProtegoError(
                f"Failed to get function.\nAWS Region not found. You can specify a region using AWS_REGION \
                    environment variable.\n {exc_info}") from exc_info
        else:
            raise ProtegoError(
                f"Failed to get function: {exc_info}") from exc_info


def validate_function_configuraion(function_arn, fsp_action, function_configuration, region):
    if fsp_action == CGRequest.Actions.Add:
        func_layers = function_configuration.get("Configuration").get(
            "Layers") if function_configuration.get("Configuration").get("Layers") else []

        # Check if function already has MAX_LAYER_SIZE
        if len(func_layers) == MAX_LAYER_SIZE:
            raise FspMaxLayerSizeError(
                f"Fsp Instrumentation Error: AWS Lambda function cannot reference more than {MAX_LAYER_SIZE} layers. As function is already having maximum number of layers, not able to apply FSP to the function")

        func_layers = [layer["Arn"] if layer.get(
            "Arn") else layer for layer in func_layers]

        # CloudGuard layer already exists, skip memory check
        if any("cloudguard-fsp-" in json.dumps(l) for l in func_layers):
            logger.info("CloudGuard layer already exists, skipping memory check")
            return
        memory_size = function_configuration.get(
            "Configuration").get("MemorySize")

        logs_client = boto3.client('logs', region_name=region)
        query_string = f"filter @type = 'REPORT' | stats {memory_size} - max(@maxMemoryUsed / 1024 / 1024) as overProvisionedMB"

        log_group_name = f"/aws/lambda/{get_function_name(function_arn)}"
        end_time = round(time.time() * 1000)
        start_time = end_time - MILLISECONDS_IN_MONTH

        try:
            start_query_response = logs_client.start_query(
                logGroupName=log_group_name,
                startTime=start_time,
                endTime=end_time,
                queryString=query_string
            )
        except Exception as exc:
            logger.info("The start_query operation failed : %s", exc)
            return

        query_id = start_query_response.get("queryId")
        if not query_id:
            return
        try:
            over_provisioned_mb = get_over_provisioned_memory(logs_client, query_id)
            logger.info("Success: get_over_provisioned_memory %s", over_provisioned_mb)
            if over_provisioned_mb and (float(over_provisioned_mb) < float(MIN_MEMORY_MB_REQUIRE)):
                raise FspUpdateFunctionNotEnoughMemoryError("This lambda requires more memory in order to add FSP")
        except Exception as exc_info:
            if isinstance(exc_info, FspUpdateFunctionNotEnoughMemoryError):
                raise FspUpdateFunctionNotEnoughMemoryError("This lambda requires more memory in order to add FSP") from exc_info
            # ignore all other error types and continue with the add fsp flow
            logger.info('Unable to get lambda over provisioned memory %s', exc_info)

def get_over_provisioned_memory(logs_client, query_id):
    tries = 0
    over_provisioned_mb = None
    while tries < MAX_NUM_OF_CW_RETRIES:
        response = logs_client.get_query_results(
            queryId=query_id
        )
        if response and response['status'] == 'Complete':
            if response.get("results", []):
                over_provisioned_mb = response.get("results")[
                    0][0]["value"]
                if over_provisioned_mb:
                    logger.info("overProvisionedMB: %s", over_provisioned_mb)
                    return over_provisioned_mb
        # If no response, try again 5 times with a 2-second sleep
        time.sleep(2)
        tries += 1
    logger.debug("No data found for lambda provisioned memory")


def get_function_name(function_arn):
    splited_function_arn = function_arn.split(':')
    return splited_function_arn[6]


def calculate_function_config(function_arn, fsp_action, function_configuration, region, runtime, res={}):
    account_id = get_account_id(function_arn)
    new_protego_env_var = {
        "PROTEGO_FSP_CUSTOMER_ACCOUNT_ID": account_id + ":" + secrets.token_hex(16)
    }

    if fsp_action == CGRequest.Actions.Add:
        fsp_layer_data = get_fsp_layer_data(function_arn, runtime, region)
        res["layer"] = fsp_layer_data["layer"]
        if "environment" in fsp_layer_data:
            new_protego_env_var.update(fsp_layer_data['environment'])

    current_env_vars = function_configuration.get(
        "Configuration", {}).get("Environment", {}).get("Variables", {})
    logger.info(f"Existing env variables : {current_env_vars}")
    new_env_vars = None
    if fsp_action == CGRequest.Actions.Remove:
        # remove FSP
        if "PROTEGO_FSP_CUSTOMER_ACCOUNT_ID" in current_env_vars:
            del current_env_vars["PROTEGO_FSP_CUSTOMER_ACCOUNT_ID"]
        if current_env_vars:
            # supported nodejs runtimes : nodejs14.x | nodejs16.x | nodejs18.x | nodejs20.x, nodejs22.x
            if runtime.startswith('nodejs'):
                current_env_vars = remove_node_options(current_env_vars)
            elif runtime == 'java8.al2' or runtime == 'java11':
                current_env_vars = remove_java_tool_options(
                    current_env_vars, runtime)
            elif runtime.startswith('dotnet'):
                current_env_vars = remove_dotnet_startup_hooks(current_env_vars)
        new_env_vars = {
            "Variables": current_env_vars
        }
    else:
        # add FSP
        new_env_vars = {
            "Variables": merge_protego_env_vars(current_env_vars, new_protego_env_var, runtime)
        }

    # handler
    # None is a valid value. will be checked at later stage
    new_handler = res.get("handler", None)
    # Layers
    # new_layer_arn = res.get("layer")
    if fsp_action == CGRequest.Actions.Add and res["layer"]:
        current_layers = function_configuration.get(
            "Configuration", {}).get("Layers", [])
        current_layers = [layer["Arn"] if layer.get(
            "Arn") else layer for layer in current_layers]
        new_layers = [
            l for l in current_layers if "cloudguard-fsp-" not in json.dumps(l)]
        new_layers.append(res["layer"])

    elif fsp_action == CGRequest.Actions.Remove:
        current_layers = function_configuration.get(
            "Configuration", {}).get("Layers", [])
        current_layers = [layer["Arn"] if layer.get(
            "Arn") else layer for layer in current_layers]
        current_layers = [
            l for l in current_layers if "cloudguard-fsp-" not in json.dumps(l)]
        current_layers = [
            l for l in current_layers if "protego-fsp-" not in json.dumps(l)]

        new_layers = current_layers
    else:
        new_layers = None  # None is a valid value. will be checked at later stage
    logger.info(f"env variables after update: {new_env_vars}")
    return new_env_vars, new_handler, new_layers


def get_account_id(function_arn):
    splited_function_arn = function_arn.split(':')
    return splited_function_arn[4]


def get_fsp_layer_data(function_arn, runtime, region):
    res = {}
    runtime_layer = None

    manifest_json = json.load(open(f"{PROTEGO_FSP_DIR_TMP}/manifest.json"))
    if runtime == 'java8.al2':
        res['environment'] = {
            JAVA_TOOL_OPTIONS: "-javaagent:" + str(JAVA8_AGENT)}
        runtime_layer = "java-layer"
    elif runtime == 'java11':
        res['environment'] = {JAVA_TOOL_OPTIONS: "-javaagent:" +
                              str(JAVA11_AGENT) + " " + "-javaagent:" + str(JAVA8_AGENT)}
        runtime_layer = "java-layer"
    elif runtime in ['python3.7', 'python3.8', 'python3.9', 'python3.10', 'python3.11']:
        runtime_layer = "python-layer"
    elif runtime in ['nodejs22.x', 'nodejs20.x', 'nodejs18.x', 'nodejs16.x', 'nodejs14.x']:
        res['environment'] = {
            NODE_OPTIONS: "--require " + NODE_PACKAGE_NAME}
        runtime_layer = "nodejs-layer"
    elif runtime == 'dotnet6':
        res['environment'] = {
            DOTNET_STARTUP_HOOKS: DOTNET_STARTUP_HOOKS_VAL}
        runtime_layer = "dotnet6-layer"
    elif runtime == 'dotnet8':
        res['environment'] = {
            DOTNET_STARTUP_HOOKS: DOTNET_STARTUP_HOOKS_VAL}
        runtime_layer = "dotnet8-layer"
    else:
        logger.info("Unsupported runtime for layers: %s", str(runtime))
        return None
    res['layer'] = manifest_json[runtime_layer][region]
    if res['layer']:
        return res

    return None


def remove_node_options(current_env_vars):
    current_node_options = current_env_vars.get(NODE_OPTIONS, None)
    if current_node_options:
        only_protego_node_options = current_node_options.replace(
            ' ', '') == NODE_OPTIONS_ENV_VAR_VALUE.replace(' ', '')
        if only_protego_node_options:
            logger.debug("Only Protego node options exist and will be removed")
            del current_env_vars[NODE_OPTIONS]
        else:
            logger.debug("other node options exist, remove only Protego")
            current_node_options = current_node_options.replace(NODE_OPTIONS_ENV_VAR_VALUE, '')
            current_env_vars[NODE_OPTIONS] = ' '.join(current_node_options.split())
    return current_env_vars


def remove_java_tool_options(current_env_vars, runtime):
    current_java_options_val = JAVA11_TOOL_OPTIONS_ENV_VAR_VALUE if runtime == "java11" else JAVA8_TOOL_OPTIONS_ENV_VAR_VALUE
    current_java_options = current_env_vars[JAVA_TOOL_OPTIONS]
    if current_java_options:
        only_protego_java_options = current_java_options.replace(
            ' ', '') == current_java_options_val.replace(' ', '')
        if only_protego_java_options:
            logger.debug(
                "Only Protego java tool options exist and will be removed")
            del current_env_vars[JAVA_TOOL_OPTIONS]
        else:
            logger.debug("other java tool options exist, remove only Protego")
            current_java_options = current_java_options.replace(current_java_options_val, '')
            current_env_vars[JAVA_TOOL_OPTIONS] = ' '.join(current_java_options.split())
    return current_env_vars


def remove_dotnet_startup_hooks(current_env_vars):
    # currentStartupHook
    current_dotnet_options = current_env_vars[DOTNET_STARTUP_HOOKS]
    if current_dotnet_options:
        only_protego_dotnet_options = current_dotnet_options.replace(
            ' ', '') == DOTNET_STARTUP_HOOKS_VAL.replace(' ', '')
        if only_protego_dotnet_options:
            logger.debug(
                "Only Protego dotnet startup hook exist and will be removed")
            del current_env_vars[DOTNET_STARTUP_HOOKS]
        else:
            logger.debug("other startup hooks exist, remove only Protego")
            separator = DOTNET_STARTUP_HOOKS_WINDOWS_SEPARATOR if isWindows() else DOTNET_STARTUP_HOOKS_UNIX_SEPARATOR
            current_dotnet_options = current_dotnet_options.replace(DOTNET_STARTUP_HOOKS_VAL, '')
            current_env_vars[DOTNET_STARTUP_HOOKS] = separator.join([part for part in current_dotnet_options.split(separator) if part])
    return current_env_vars


def isWindows():
    return True if platform.system() == "Windows" else False


def merge_protego_env_vars(current_env_vars, new_env_var, runtime):
    if new_env_var:
        if runtime.startswith('nodejs'):
            node_options = merge_options(current_env_vars.get(
                NODE_OPTIONS, None), new_env_var.get(NODE_OPTIONS, None))
            current_env_vars.update(new_env_var)
            if node_options:
                current_env_vars[NODE_OPTIONS] = node_options
            return current_env_vars if current_env_vars else {}
        if runtime.startswith('dotnet'):
            dotnet_options = merge_options(
                current_env_vars.get(DOTNET_STARTUP_HOOKS, None), new_env_var.get(DOTNET_STARTUP_HOOKS, None), True)
            current_env_vars.update(new_env_var)
            if dotnet_options:
                current_env_vars[DOTNET_STARTUP_HOOKS] = dotnet_options
            return current_env_vars if current_env_vars else {}
        if runtime.startswith('java'):
            java_tool_options = merge_options(
                current_env_vars.get(JAVA_TOOL_OPTIONS, None), new_env_var.get(JAVA_TOOL_OPTIONS, None))
            logger.debug("java original Envs %s", str(current_env_vars))
            logger.debug("java new Envs %s", str(new_env_var))
            current_env_vars.update(new_env_var)
            if java_tool_options:
                current_env_vars[JAVA_TOOL_OPTIONS] = java_tool_options
            return current_env_vars if current_env_vars else {}

        current_env_vars.update(new_env_var)
        return current_env_vars if current_env_vars else {}


def merge_options(current_options, fsp_options, is_runtime_dotnet=False):
    if not current_options:
        logger.debug("No tool options exist")
        return fsp_options

    if current_options == fsp_options:
        logger.debug("Tool options exist with the same value %s",
                     current_options)
        return current_options

    merged_options = ""
    if is_runtime_dotnet:
        separator = ";" if isWindows() else ":"
        re_pattern = re.compile(fr'{fsp_options}{separator}|{fsp_options}$', re.MULTILINE)
        merged_options = f"{fsp_options}{separator}{re.sub(re_pattern, '', current_options)}"
    else:
        re_pattern = re.compile(fr"{fsp_options}[\s]|{fsp_options}$")
        merged_options = f"{fsp_options} {re.sub(re_pattern, '', current_options)}"

    return re.sub(r'\s+|\s$', ' ', merged_options)


def update_function_config(function_arn, new_env_var, new_handler, new_layers, region):
    params = {}
    if new_env_var:
        params["Environment"] = new_env_var
    if new_handler:
        params["Handler"] = new_handler
    if new_layers or len(new_layers) == 0:
        params["Layers"] = new_layers
    if not function_arn:
        return None
    try:
        return update_lambda_function_configuration(region, function_arn, params)

    except Exception as exc_info:
        if "AccessDeniedException" in str(exc_info):
            raise FspAccessDeniedError(
                f"Fsp Instrumentation Error: {exc_info}") from exc_info
        if "InvalidParameterValueException" in str(exc_info):
            # handling exception for unzipped code max size combined with layer
            if "Function code combined with layers exceeds the maximum allowed size" in str(exc_info):
                raise FspCombinedCodeLayerSizeIsTooLargeError(
                    f"Fsp Instrumentation Error: {exc_info}") from exc_info
            # handling exception for unzipped code max size
            if "Code uncompressed size is greater than max allowed size" in str(exc_info):
                raise FspCodeSizeIsTooLargeError(
                    f"Fsp Instrumentation Error: {exc_info}") from exc_info
        raise FspUpdateFunctionConfigError(f"Failed to update Function Config: {exc_info}") from exc_info


def update_lambda_function_configuration(region, function_arn, params):
    lambda_client = boto3.client('lambda', region_name=region)
    waiter = lambda_client.get_waiter('function_updated')
    waiter.wait(FunctionName=function_arn)

    return lambda_client.update_function_configuration(
        FunctionName=function_arn, **params)
