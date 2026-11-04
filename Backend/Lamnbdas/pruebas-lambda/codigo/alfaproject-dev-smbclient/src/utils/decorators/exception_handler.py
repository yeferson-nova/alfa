import time
import traceback
from src.utils.handler.logger_base import log


def handle_exception(
    function_name:str,
    step_execution:str,
    exception_class=Exception
    ):
    """
    
    Args:
        - function_name (str): The function's name that is executed.
        - step_execution (str): Attr to help us to know the step code.
        - exception_class (Any): Exception class, default class is Exception.
        
    Returns:
        - (Any): The function's return.
    """
    
    def validate_exception(function):
        def wrapper(*args, **kwards):
            
            start_time = time.time()
            log.info(f"🟡 Function: {function_name}, start execution.")

            try:
                result = function(*args, **kwards)

            except Exception as e:
                traceback_details = traceback.format_exc()
                traceback_details = traceback_details if len(traceback_details) < 10000 else traceback_details[:10000]
                log.error(f"🔴 Error, execution failed in step: {step_execution}, Error: {traceback_details}")
                raise exception_class(e)

            log.info(f"🟢 Function: {function_name}, end execution")
            end_time = time.time()

            elapsed_time = end_time - start_time
            log.info(f"🔵 Function: {function_name}, Execution time: {round(elapsed_time, 2)} Sec.")

            return result
        return wrapper
    return validate_exception