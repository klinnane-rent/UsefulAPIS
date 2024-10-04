import difflib

import requests
from requests import HTTPError

from api_library.base_api_client import BaseAPIClient
from api_library.google_api_client import GoogleMapsAPIClient
from constants.region_id_neighborhood_mapping import REGION_ID_NEIGHBORHOOD_MAPPING
from util.logger import get_logger

logger = get_logger(__name__)


class LocationsAPIClient(BaseAPIClient):
    def __init__(self):
        super().__init__(api_key="data/keys/geodb_api_key.txt")
        self.endpoint = "https://wft-geo-db.p.rapidapi.com/v1"

        self.headers = {
            "X-RapidAPI-Key": self.key,
            "X-RapidAPI-Host": "wft-geo-db.p.rapidapi.com"
        }

    def get_city_from_coordinates(self, lat, lon, city_state: str):
        if lat is None or lon is None:
            logger.error(f"Unable to get city from coordinates: {lat}, {lon}")
            return None
        else:
            logger.info(f"Getting city from coordinates: {lat:+}, {lon:+}")
            params = {
                "location": f"{lat:+}{lon:+}",
                "radius": 10,
                "countryIds": "US",
                "languageCode": "en",
                "limit": 25,
                "types": "CITY"
            }
            try:
                response = requests.get(f"{self.endpoint}/geo/cities", headers=self.headers, params=params)
            except HTTPError as err:
                logger.exception(err)
                raise RuntimeError

            if response.ok:
                cities = response.json()["data"]
                try:
                    possible_cities = [city.get("city") for city in cities]
                    logger.debug(f"Possible cities: {possible_cities}")
                    city = str(difflib.get_close_matches(city_state.split(", ")[0], possible_cities, cutoff=0.8)[0])
                    logger.info(f"Found city: {city}")
                    for city_ in cities:
                        if city_["city"] == city:
                            city = city_
                            break
                except IndexError as err:
                    logger.exception(err)
                    city = None
                    logger.warn(f"Retrying and looking up {city_state} in the list of neighborhoods")
                    if city_state in REGION_ID_NEIGHBORHOOD_MAPPING.keys():
                        city = city_state
                return city
            else:
                logger.exception(f"GeoDB API {response.status_code} error: {response.text}")
                raise RuntimeError

    def get_nearby_cities(
            self,
            city_id: str,
            radius: int = 50,
            limit: int = 100,
            min_population: int = 20000,
            max_population: int = 99999999,
    ):
        """
        Current GeoDB Cities API plan has the following limits and possible increases

        1 request/second -> 10 requests/second
        Max search radius of 100 miles -> 500 miles
        Max 10 results -> 100
        1000 requests/day -> 200,000 requests/day
        """
        logger.info("Getting nearby cities")

        params = {
            "radius": radius,
            "limit": limit,
            "minPopulation": min_population,
            "maxPopulation": max_population,
            "countryIds": "US",
            "languageCode": "en",
            "types": "CITY",
        }

        try:
            response = requests.get(
                url=f"{self.endpoint}/geo/cities/{city_id}/nearbyCities",
                headers=self.headers,
                params=params,
            )
        except HTTPError as err:
            logger.exception(err)
            raise RuntimeError

        if response.ok:
            return response.json()["data"]
        else:
            logger.exception(f"GeoDB API {response.status_code} error: {response.reason}: {response.json()['message']}")
            raise RuntimeError

    def get_api_calls(self):
        """
        Easy way of getting population information.
        When retrieving nearby cities, population is already calculated by GeoDB Cities API.
        This fake API call just uses that data instead of hitting the network
        """
        class Fake:
            pass

        population = Fake()
        setattr(population, "__name__", "population")
        return [population]
