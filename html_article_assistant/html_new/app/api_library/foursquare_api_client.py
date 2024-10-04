import urllib.parse
import requests

# distance is in meters, popularity is [0,1], price is 1-4
import us.states
from requests import HTTPError

from api_library.base_api_client import BaseAPIClient
from constants.enums.foursquare_field_enum import FoursquareField
from constants.enums.foursquare_sort_enum import FoursquareSortEnum
from model.foursquare_place import FoursquarePlace
from util.logger import get_logger

logger = get_logger(__name__)


class FoursquareAPIClient(BaseAPIClient):
    """
    Client for the Foursquare Places API
    https://developer.foursquare.com/docs/places-api-overview
    """
    def __init__(self):
        super().__init__(api_key="data/keys/foursquare_api_key.txt")

    def search(
            self,
            ll: str = None,
            query: str = None,
            radius: int = None,
            exclude_all_chains: bool = True,
            name: bool = False,
            location: bool = False,
            categories: bool = False,
            distance: bool = False,
            email: bool = False,
            website: bool = False,
            rating: bool = False,
            popularity: bool = False,
            price: bool = False,
            min_price: int = None,
            max_price: int = None,
            open_at: str = None,
            near: str = None,
            sort: FoursquareSortEnum = FoursquareSortEnum.RELEVANCE,
            limit: int = None,
    ) -> list[FoursquarePlace] | None:
        all_fields = [
            name,
            location,
            categories,
            distance,
            email,
            website,
            rating,
            popularity,
            price,
        ]
        fields = [field_name for field_name, is_present in zip(FoursquareField.list(), all_fields) if is_present]
        fields = "name," + ",".join([field.value for field in fields if field != FoursquareField.NAME])
        sort = sort.value

        params = locals().copy()
        params.pop("self")
        params.pop("all_fields")
        if near:
            city, state, *_ = near.split(", ")
            state = us.states.lookup(state)
            if not state:
                return None
            near = f"{city}, {state}"

        for field_name in FoursquareField.list():
            params.pop(field_name.value)
        params_copy = params.copy()
        for key, value in params_copy.items():
            if value is None:
                params.pop(key)

        logger.info(f"Calling Foursquare Places Search API with args: {params}")
        params_str = urllib.parse.urlencode(params)
        url = f"https://api.foursquare.com/v3/places/search?{params_str}"
        headers = {
            "Accept": "application/json",
            "Authorization": self.key,
        }
        try:
            response = requests.get(url, headers=headers)
        except HTTPError as err:
            logger.exception(err)
            return None

        if response.ok:
            raw_results = response.json()["results"]
            if not raw_results:
                results = None
            else:
                results = [FoursquarePlace(result) for result in raw_results]
            return results
        else:
            logger.exception(f"Foursquare API {response.status_code} error: {response.reason}: {response.json()}")
            return None

    def get_number_of_nearby_places(self, query: str, ll: str):
        places = self.search(
            query=query,
            name=True,
            radius=4000,
            ll=ll,
            limit=50
        )
        if not places:
            result = 0
        else:
            result = len(places)
        return result

    def get_api_calls(self):
        calls = [
            self.get_number_of_nearby_places,
        ]
        return calls
