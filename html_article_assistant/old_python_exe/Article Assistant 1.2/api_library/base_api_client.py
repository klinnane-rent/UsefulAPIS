from util.logger import get_logger

logger = get_logger(__name__)


class BaseAPIClient:
    def __init__(self, api_key: str = None, log=False):
        if log:
            logger.debug(f"Creating {self.__class__.__name__}")
        with open(api_key) as file:
            self.key = file.read()

    def get_api_calls(self):
        raise NotImplementedError
