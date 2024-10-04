from api_library.base_api_client import BaseAPIClient
from api_library.redfin_api_client import RedfinAPIClient
from api_library.walkscore_api_client import WalkscoreAPIClient
from constants.api_list import API_CLIENT_LIST


class API:
    def __init__(
            self,
            api_client: BaseAPIClient,
            api_method: str,
            weight: float,
            column_name: str,
            ascending: bool = False,
            args: str = None,
    ):
        if api_client not in API_CLIENT_LIST:
            raise ValueError(f"API Client type '{api_client.__name__}' is invalid")

        api_method = "_".join(api_method.lower().split())
        if api_method not in [method.__name__ for method in api_client().get_api_calls()]:
            raise ValueError(f"API '{api_method}' does not exist for client {api_client.__name__}")

        self.client = api_client
        self.method = api_method
        self.weight = weight
        self.column_name = column_name
        self.descending = ascending
        self.args = args

    def call_api(self, kwargs):
        func = getattr(self.client, self.method)
        return func(self=self.client(), **kwargs)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"{self.client.__name__}.{self.method}()"
