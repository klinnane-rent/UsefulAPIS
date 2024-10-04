import itertools

from api_library.census_api_client import CensusAPIClient
from api_library.foursquare_api_client import FoursquareAPIClient
from api_library.geo_database_client import LocationsAPIClient
from api_library.google_api_client import GoogleMapsAPIClient
from api_library.redfin_api_client import RedfinAPIClient
from api_library.tourist_api_client import TouristAPIClient
from api_library.walkscore_api_client import WalkscoreAPIClient
from constants.constants import NULL

from model.api import API
from model.ranking_table import Table, RankedItem
from util.logger import get_logger

logger = get_logger(__name__)


def information_about_city(city: str, state: str, methodologies: list[API]) -> list[list]:
    lat, lon = GoogleMapsAPIClient().address_to_coordinates(f"{city} {state}")
    city_state = {
        "city": city,
        "name": city,
        "region": state,
        "latitude": lat,
        "longitude": lon,
        "population": NULL,
    }
    if not lat and not lon:
        logger.exception(f"Unable to geocode location: '{city}, {state}'")
        city_state = {
            "city": city,
            "name": city,
            "region": state,
            "latitude": NULL,
            "longitude": NULL,
            "population": NULL,
        }
    # else:
    #     client = LocationsAPIClient()
    #     city_state = client.get_city_from_coordinates(lat, lon, city)
    #     if not city_state:
    #         city_state = {
    #             "city": city,
    #             "name": city,
    #             "region": state,
    #             "latitude": lat,
    #             "longitude": lon,
    #             "population": NULL,
    #         }
    table = Table(initial_items=[city_state["city"]])
    print(table)
    for i, methodology in enumerate(methodologies):
        if methodology.client == WalkscoreAPIClient:
            args = {"address": f'{city_state["city"]}, {city_state["region"]}'}
            result = methodology.call_api(args)
        elif methodology.client == FoursquareAPIClient:
            args = {"ll": f"{city_state['latitude']},{city_state['longitude']}", "query": methodology.args}
            result = methodology.call_api(args)
            methodology.column_name = methodology.column_name.replace("Places", methodology.args.title())
        elif methodology.client == LocationsAPIClient:
            result = city_state["population"]
            methodology.column_name = "Population"
        elif methodology.client == CensusAPIClient:
            methodology.column_name = methodology.args
            args = {"cities": [city_state["name"]], "state": city_state["region"], "selection": methodology.args}
            result = methodology.call_api(args)
            if result:
                result = result[0]
        elif methodology.client == RedfinAPIClient:
            args = {"neighborhood": None,"city": city_state["name"], "state": city_state["region"], "metric": methodology.args}
            result = methodology.call_api(args)
            methodology.column_name = methodology.args
        elif methodology.client == TouristAPIClient:
            args = {"city_dict": city_state}
            results = methodology.call_api(args)
            table.add_column(
                "Nearby Attractions",
                methodology.weight,
                [RankedItem(city_state["city"], result) for result in results],
                methodology.descending
            )
            continue
        else:
            result = NULL

        if result is None:
            result = NULL

        table.add_column(
            methodology.column_name,
            methodology.weight,
            [RankedItem(city_state["city"], result)],
            methodology.descending
        )

    display_table = []
    for column, methodology in zip(table.columns, methodologies):
        display_table.append([methodology.column_name])
        for item in column.items:
            display_table[-1].append(item.value)
    display_table = list(map(list, itertools.zip_longest(*display_table, fillvalue="")))

    return display_table
