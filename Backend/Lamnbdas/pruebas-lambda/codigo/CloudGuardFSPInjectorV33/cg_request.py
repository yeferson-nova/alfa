# CGRequest


class CGRequest:
    def __init__(self, action=None, function_arn=None, fsp_version=None, fsp_bundle_url=None):
        self.action = action
        self.functionArn = function_arn
        self.fspVersion = fsp_version
        self.fspBundleUrl = fsp_bundle_url

    def get_action(self):
        return self.action

    def set_action(self, action):
        self.action = action

    def get_function_arn(self):
        return self.functionArn

    def set_function_arn(self, function_arn):
        self.functionArn = function_arn

    def get_fsp_version(self):
        return self.fspVersion

    def set_fsp_version(self, fsp_version):
        self.fspVersion = fsp_version

    def get_fsp_bundle_url(self):
        return self.fspBundleUrl

    def set_fsp_bundle_url(self, fsp_bundle_url):
        self.fspBundleUrl = fsp_bundle_url

    def to_str(self):
        return f"{self.functionArn} {self.action}"

    def to_json(self):
        return self.__dict__

    class Actions:
        Add = "ADD_FSP"
        Remove = "REMOVE_FSP"

        @staticmethod
        def get_plugin_action(action):
            if action == CGRequest.Actions.Add:
                return "add"
            elif action == CGRequest.Actions.Remove:
                return "remove"
            return None
