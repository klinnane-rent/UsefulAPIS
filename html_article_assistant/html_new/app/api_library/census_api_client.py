from functools import lru_cache

import requests
import us
import difflib

from api_library.base_api_client import BaseAPIClient
from constants.census_variable_mapping import CENSUS_VARIABLE_NAMES
from constants.constants import CITY_MAPPINGS_FIPS, NULL
from util.logger import get_logger

logger = get_logger(__name__)


class CensusAPIClient(BaseAPIClient):
    key = ""
    endpoint = "https://api.census.gov/data/2020/acs/acs5"

    def __init__(self):
        super().__init__(api_key="data/keys/census_api_key.txt")
        CensusAPIClient.key = self.key
        self.endpoint = "https://api.census.gov/data/2020/acs/acs5"

    @staticmethod
    @lru_cache(maxsize=64)
    def get_city_fips_mapping(state: str):
        logger.info(f"Getting the FIPS code mapping for cities in state: '{state}'")
        # Get list of cities
        found_state = us.states.lookup(state)
        if not found_state:
            logger.error(f"Unable to lookup state: '{state}'")
            fips_mapping = None
        else:
            state_fips = int(found_state.fips)
            variables = ["NAME"]
            response = requests.get(
                url=f"{CensusAPIClient.endpoint}/subject",
                params={
                    "get": ",".join(variables),
                    "for": "place:*",
                    "in": f"state:{state_fips:02d}",
                    "key": CensusAPIClient.key
                }
            )

            # Result headers are ["City, State", "FIPS state code", "FIPS 55 place code"]
            if response.ok:
                results = response.json()[1:]
            else:
                raise ValueError(f"{response.status_code} error: {response.content}")

            """
            result = [
                ...
                ["Bainbridge Island city, Washington", "53", "03736"],
                ...
                ["Seattle city, Washington", "53", "63000"],
                ...
            ]
            
            fips_mapping = {
                ...
                "Bainbridge Island": "03736",
                ...
                "Seattle": "63000",
                ...
            ]
            """
            if results:
                fips_mapping = dict()
                for result in results:
                    location, fips_code = result[0], result[2]
                    city, state = location.split(",")
                    city = " ".join(city.split()[:-1])
                    fips_mapping[city] = fips_code
            else:
                raise ValueError("FIPS mapping unavailable")
        return fips_mapping

    def get_census_data(self, cities: list[str], state: str, selection: str):
        # https://api.census.gov/data/2020/acs/acs5/subject?get=NAME,S0101_C01_001E&for=place:*&in=state:53
        # 2021/acs/acs1/profile?get=group(DP02)&for=us:1&key=YOUR_KEY_GOES_HERE
        # Spaghetti below
        state_city_fips_mapping = self.get_city_fips_mapping(state)
        if not state_city_fips_mapping:
            results = [NULL] * len(cities)
        else:
            city_fips_mapping = dict()
            for city in cities:
                # possible_cities = dict()
                # for _city, fips_code in state_city_fips_mapping.items():
                #     if city in _city:
                #         possible_cities[_city] = fips_code
                state_abbr = us.states.lookup(state).abbr
                for i, city_ in enumerate(cities):
                    if f"{city_.lower()}, {state_abbr.lower()}" in CITY_MAPPINGS_FIPS:
                        city = CITY_MAPPINGS_FIPS[f"{city_.lower()}, {state_abbr.lower()}"]
                        cities[i] = city
                closest_matches = difflib.get_close_matches(city, state_city_fips_mapping.keys())
                if closest_matches:
                    closest_match = str(closest_matches[0])
                    city_fips_mapping[city] = state_city_fips_mapping[closest_match]
                else:
                    city_fips_mapping[city] = None

            city_fips_mapping_copy = city_fips_mapping.copy()
            for city, fips_code in city_fips_mapping.items():
                if fips_code is None:
                    city_fips_mapping_copy.pop(city)
            state_code = int(us.states.lookup(state).fips)
            city_names = ', '.join(city_fips_mapping_copy.keys())
            logger.info(f"Looking up '{selection}' for cities: '{city_names}' in state: '{state}'")
            response = requests.get(
                url=f"{self.endpoint}/profile",
                params={
                    "get": ",".join([CENSUS_VARIABLE_NAMES[selection]]),
                    "for": f"place:{','.join(city_fips_mapping_copy.values())}",
                    "in": f"state:{state_code:02d}",
                    "key": self.key
                }
            )
            if response.ok:
                results = response.json()[1:]
                result_mapping = dict()
                fips_city_mapping = {value: key for key, value in city_fips_mapping.items()}
                for result, _, fips_code in results:
                    result_mapping[fips_city_mapping[fips_code]] = result
                city_result_mapping = dict()
                for city in city_fips_mapping.keys():
                    city_result_mapping[city] = result_mapping.get(city, 0)
            else:
                logger.error(f"{response.status_code} error: {response.text}")
                return None
            results = [float(city_result_mapping[city]) for city in cities]
        return results

    def get_api_calls(self):
        return [
            self.get_census_data
        ]
