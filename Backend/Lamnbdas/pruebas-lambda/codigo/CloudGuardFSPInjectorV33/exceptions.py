DEFAULT_ERROR_ID = 99

errors_mapping = {
    "FspCodeSizeIsTooLargeError": {
        "id": 139
    },
    "FspMaxLayerSizeError": {
        "id": 141
    },
    "FspCombinedCodeLayerSizeIsTooLargeError": {
        "id": 142
    },
    "FspAccessDeniedError": {
        "id": 144
    }
}

# Exceptions
    
class FspInjectorException(Exception):
    """Exception FspInjectorException"""
    def __init__(self, message):
        super(FspInjectorException, self).__init__(message)
        self.message = message


class FspInjectorUnexpectedException(Exception):
    """Exception FspInjectorUnexpectedException"""

    def __init__(self, message):
        super(FspInjectorUnexpectedException, self).__init__(message)
        self.message = message


class FspResourceConflictStateError(Exception):
    """Exception FspResourceConflictStateError"""

    def __init__(self, message):
        super(FspResourceConflictStateError, self).__init__(message)
        self.message = message


class ProtegoError(Exception):
    """Exception ProtegoError"""

    def __init__(self, message):
        super(ProtegoError, self).__init__(message)
        self.message = message


class ProtegoBaseErrorException(Exception):
    """Exception ProtegoBaseErrorException"""

    def __init__(self, message):
        super(ProtegoBaseErrorException, self).__init__(message)
        self.message = message


class FspUpdateFunctionNotEnoughMemoryError(Exception):
    """Exception FspUpdateFunctionNotEnoughMemoryError"""

    def __init__(self, message):
        super(FspUpdateFunctionNotEnoughMemoryError, self).__init__(message)
        self.message = message


class FspAccessDeniedError(Exception):
    """Exception FspAccessDeniedError"""

    def __init__(self, message):
        super(FspAccessDeniedError, self).__init__(message)
        self.message = message


class FspMaxLayerSizeError(Exception):
    """Exception FspMaxLayerSizeError"""

    def __init__(self, message):
        super(FspMaxLayerSizeError, self).__init__(message)
        self.message = message


class FspCombinedCodeLayerSizeIsTooLargeError(Exception):
    """Exception FspCombinedCodeLayerSizeIsTooLargeError"""

    def __init__(self, message):
        super(FspCombinedCodeLayerSizeIsTooLargeError, self).__init__(message)
        self.message = message


class FspCodeSizeIsTooLargeError(Exception):
    """Exception FspCodeSizeIsTooLargeError"""

    def __init__(self, message):
        super(FspCodeSizeIsTooLargeError, self).__init__(message)
        self.message = message


class FspUpdateFunctionConfigError(Exception):
    """Exception FspUpdateFunctionConfigError"""

    def __init__(self, message):
        super(FspUpdateFunctionConfigError, self).__init__(message)
        self.message = message
