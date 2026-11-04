# CGResponse
from cg_request import CGRequest

class CGResponse:
    def __init__(self, message, last_modified_time, request):
        self.message = message
        self.lastModified = last_modified_time
        self.request = request

    def get_message(self):
        return self.message

    def get_last_modified(self):
        return self.lastModified

    def get_request(self):
        return self.request

    def to_str(self):
        return f"{self.message} {self.lastModified} {self.request.to_str()}"

    def to_json(self):
        return {
            'message': self.message,
            'lastModified': self.lastModified,
            'request': self.request.to_json()
        }