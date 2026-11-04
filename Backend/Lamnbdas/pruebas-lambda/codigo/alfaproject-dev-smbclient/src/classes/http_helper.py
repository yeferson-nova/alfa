import traceback


class HttpHelper:
    
    @classmethod   
    def execution_successfull(
        cls,
        details:str="",
        status_code:int=200,
        ):
        
        return {
            "body": {
                "statusCode": status_code,
                "message": "execution successfull",
                "warningDetails": details
            }
        }
    
    @classmethod
    def execution_failed(
        cls,
        status_code:int=500,
        details:str="",
        ):
        
        return {
            "body": {
                "statusCode": status_code,
                "message": "execution failed",
                "exceptionDetails": details,
            }
        }