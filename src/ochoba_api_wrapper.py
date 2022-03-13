from requests_toolbelt import sessions


class OchobaApiWrapper:
    def __init__(self, config):
        self.url = config["url"]
        self.session = sessions.BaseUrlSession(base_url=self.url)

    def execute(self, endpoint):
        return self.session.get(endpoint)
