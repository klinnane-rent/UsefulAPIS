from api_library.census_api_client import CensusAPIClient
from api_library.foursquare_api_client import FoursquareAPIClient
from api_library.google_api_client import GoogleMapsAPIClient
from api_library.redfin_api_client import RedfinAPIClient
from api_library.tourist_api_client import TouristAPIClient
from constants.api_list import API_CLIENT_LIST
from constants.census_variable_mapping import CENSUS_VARIABLE_NAMES
from constants.redfin_metric_mapping import REDFIN_METRIC_MAPPING
from model.api import API

ARGS_WIDTH = 300

def get_api_calls(api_name: str):
    api_client_module = __import__("constants.api_list", fromlist=[api_name])
    api_client_class = getattr(api_client_module, api_name)
    api_client_instance = api_client_class()
    api_call_list = [api_call_.__name__ for api_call_ in api_client_instance.get_api_calls()]
    api_calls = [" ".join(api_call_.split('_')).title() for api_call_ in api_call_list]
    return api_calls

def update_calls(api_name: str):
    api_calls = get_api_calls(api_name)
    args = []
    if api_name == "CensusAPIClient":
        args = list(CENSUS_VARIABLE_NAMES.keys())
    elif api_name == "RedfinAPIClient":
        args = list(REDFIN_METRIC_MAPPING.keys())
    return api_calls, args

class APIProcessor:
    def __init__(self, ranked: bool = False):
        self.headers = {
            "API Client": "ResizeToContents",
            "API Call": "ResizeToContents",
            "Weight (0 - 100)": "ResizeToContents",
            "Arguments": "ResizeToContents",
            "Order": "ResizeToContents",
            "Remove Row": "ResizeToContents",
        }
        self.ranked = ranked
        if not self.ranked:
            self.headers.pop("Order")
        self.data = []

    def add_row(self, api_client: str = None, enabled: bool = True):
        api_client_list_copy = API_CLIENT_LIST.copy()
        if not self.ranked:
            api_client_list_copy.remove(GoogleMapsAPIClient)
            api_client_list_copy.remove(FoursquareAPIClient)
        else:
            api_client_list_copy.remove(TouristAPIClient)
        api_client_list = [api_.__name__ for api_ in api_client_list_copy]

        if api_client not in api_client_list:
            api_client = api_client_list[0]

        api_calls, args = update_calls(api_client)
        row = {
            "API Client": api_client,
            "API Call": api_calls[0],
            "Weight": 1,
            "Arguments": args[0] if args else "",
            "Order": False if self.ranked else None
        }
        self.data.append(row)

    def update_row(self, index, api_client, api_call, weight, arguments, order):
        if index < len(self.data):
            self.data[index] = {
                "API Client": api_client,
                "API Call": api_call,
                "Weight": weight,
                "Arguments": arguments,
                "Order": order
            }

    def remove_row(self, index):
        if index < len(self.data):
            self.data.pop(index)

    def get_values(self):
        values = []
        for row in self.data:
            api_name = row["API Client"]
            api_client_module = __import__("constants.api_list", fromlist=[api_name])
            api_client = getattr(api_client_module, api_name)
            if api_client == FoursquareAPIClient:
                args = row["Arguments"] if row["Arguments"] else "restaurants"
            elif api_client == CensusAPIClient:
                args = row["Arguments"]
            elif api_client == RedfinAPIClient:
                args = row["Arguments"]
            else:
                args = ""

            item = API(
                api_client=api_client,
                api_method=row["API Call"],
                weight=float(row["Weight"]),
                column_name=" ".join(row["API Call"].split()[1:]),
                ascending=row["Order"] if row["Order"] is not None else False,
                args=args,
            )
            values.append(item)
        return values

if __name__ == "__main__":
    processor = APIProcessor(ranked=True)
    processor.add_row(api_client="CensusAPIClient")
    processor.add_row(api_client="RedfinAPIClient")
    print("Initial Data:")
    print(processor.get_values())

    # Update a row
    processor.update_row(0, "CensusAPIClient", "Get Population", 50, "Population", True)
    print("\nUpdated Data:")
    print(processor.get_values())

    # Remove a row
    processor.remove_row(1)
    print("\nData after removing a row:")
    print(processor.get_values())
