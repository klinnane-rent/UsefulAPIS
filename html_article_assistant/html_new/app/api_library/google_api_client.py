from __future__ import annotations

import googlemaps
import numpy as np
from api_library.base_api_client import BaseAPIClient
from model.address import Address
from util.logger import get_logger

logger = get_logger(__name__)


class GoogleMapsAPIClient(BaseAPIClient):
    """
    Client for the Google Maps API
    https://developers.google.com/maps/documentation
    """
    def __init__(self):
        super().__init__(api_key="data/keys/google_api_key.txt")
        self._client = googlemaps.Client(key=self.key)

    def get_distances_and_durations(self, origin: str, destinations: list[str]) -> (list[str], list[str]):
        logger.info(f"Getting distance matrix for origin: {origin} and destinations: {destinations}")
        distances = []
        durations = []
        for i in range(0, len(destinations), 25):
            destinations_chunk = destinations[i:i + 25]
            response = self._client.distance_matrix(
                origins=[origin],
                destinations=destinations_chunk,
                units="imperial"
            )
            elements: list = response["rows"][0]["elements"]
            for element in elements:
                if element["status"] == "OK":
                    distance = element["distance"]["text"].replace("mi", "miles")
                    duration = str(round(element["duration"]["value"] / 60, 2)) + " minutes"
                    distances.append(distance)
                    durations.append(duration)
                else:
                    distances.append(0)
                    durations.append(0)
        return distances, durations

    def address_to_coordinates(self, address: Address | str) -> (float, float):
        logger.info(f"Reverse geocoding address: {address}")
        geocode_result = self._client.geocode(str(address))
        if geocode_result:
            coordinates = geocode_result[0]
        else:
            coordinates = {
                "geometry": {
                    "location": {
                        "lat": None,
                        "lng": None,
                    }
                }
            }
        location = coordinates["geometry"]["location"]
        latitude = location["lat"]
        longitude = location["lng"]
        return latitude, longitude

    def get_miles_from_location(self, location: str, destinations: list[str]) -> list[float]:
        logger.info(f"Getting distance between {location} and {destinations}")
        
        distances = self.get_distances_and_durations(location, destinations)[0]

        for num, distance in enumerate(distances):
            if type(distance) == int:
                distances[num] = None
        distances = [float(distance.strip("miles").strip('ft')) if distance else 'No Data' for distance in distances]
        logger.info(f"Distances: {distances}")
        return distances

    def get_minutes_from_location(self, location: str, destinations: list[str]) -> list[float]:

        logger.info(f"Getting duration between {location} and {destinations}")

        durations = self.get_distances_and_durations(location, destinations)[1]
        for num, duration in enumerate(durations):
            if type(duration) == int:
                durations[num] = None
        durations = [float(duration.strip("minutes")) if duration else 'No Data' for duration in durations]
        logger.info(f"Durations: {durations}")
        return durations

    # def get_nearby_neighborhoods(self, location: (float, float), radius_in_meters: int):
    #     logger.info(f"Getting neighborhoods near {location}")
    #     nearby_places = self._client.places(location=location, radius=radius_in_meters, type="neighborhood")
    #     return nearby_places

    def get_api_calls(self):
        calls = [
            self.get_miles_from_location,
            self.get_minutes_from_location,
        ]
        return calls
