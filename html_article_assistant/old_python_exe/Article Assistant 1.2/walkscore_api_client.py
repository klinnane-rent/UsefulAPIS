import urllib.parse

import requests

from api_library.base_api_client import BaseAPIClient
from api_library.google_api_client import GoogleMapsAPIClient
from model.address import Address
from util.logger import get_logger



WALK_SCORE = "walk_score"
BIKE_SCORE = "bike_score"
TRANSIT_SCORE = "transit_score"


class WalkscoreAPIClient(BaseAPIClient):
    """
    Client for the WalkScore API
    https://www.walkscore.com/professional/api.php
    """
    def __init__(self):
        print()
        super().__init__(api_key="data/keys/walkscore_api_key.txt")

    def get_score_dictionary(self, address: Address, transit=1, bike=1) -> dict[str: int]:
        print('here')
        params = locals().copy()
        googlemaps_api_client = GoogleMapsAPIClient()
        latitude, longitude = googlemaps_api_client.address_to_coordinates(address)
        print(latitude)
        if not latitude and not longitude:
            score_dict = {
                # WALK_SCORE: None,
                # BIKE_SCORE: None,
                # TRANSIT_SCORE: None,
                WALK_SCORE: 0,
                BIKE_SCORE: 0,
                TRANSIT_SCORE: 0,
            }
        else:
            params |= {"lat": latitude, "lon": longitude, "wsapikey": self.key, "format": "json"}
            params.pop("self")

            params_str = urllib.parse.urlencode(params)
            url = f"https://api.walkscore.com/score?{params_str}"
            response = requests.get(url).json()

            walk_score = response.get("walkscore", 0)

            bike_score = response["bike"]["score"] if response.get("bike") else 0

            transit_score = response["transit"]["score"] if response.get("transit") else 0
                
            score_dict = {
                WALK_SCORE: walk_score,
                BIKE_SCORE: bike_score,
                TRANSIT_SCORE: transit_score,
            }
            print(score_dict)
            self.get_api_calls()
            print(score_dict)
        return score_dict

    def get_walk_score(self, address: Address) -> int:
        print(f"Getting walk score for '{address}'")
        walk_score = self.get_score_dictionary(address)[WALK_SCORE]
        if not walk_score:
            print(f"Unable to retrieve walk score for '{address}'")
        return walk_score

    def get_bike_score(self, address: Address) -> int:
        print(f"Getting bike score for '{address}'")
        bike_score = self.get_score_dictionary(address)[BIKE_SCORE]
        if not bike_score:
            print(f"Unable to retrieve bike score for '{address}'")
        return bike_score

    def get_transit_score(self, address: Address) -> int:
        print(f"Getting transit score for '{address}'")
        transit_score = self.get_score_dictionary(address)[TRANSIT_SCORE]
        if not transit_score:
            print(f"Unable to retrieve transit score for '{address}'")
        return transit_score

    def get_api_calls(self):
        calls = [
            self.get_walk_score,
            self.get_bike_score,
            self.get_transit_score,
        ]
        return calls

if __name__ == '__main__':
    print('hieddd')
    client = WalkscoreAPIClient()
    print(client.get_score_dictionary("Alpharetta, GA, 30022"))
   

    print('done')
