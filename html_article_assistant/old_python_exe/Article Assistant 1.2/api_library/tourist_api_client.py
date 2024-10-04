import urllib.parse

import requests

from api_library.geo_database_client import LocationsAPIClient
from constants.constants import NULL
from util.logger import get_logger

logger = get_logger(__name__)


class TouristAPIClient:
    def get_nearby_tourist_attractions(self, city_dict: dict):
        nearby_cities = LocationsAPIClient().get_nearby_cities(city_id=city_dict["id"], radius=10)
        nearby_city_wikidata_ids = [city["wikiDataId"] for city in nearby_cities] + [city_dict["wikiDataId"]]
        sparql_list = [f"{{?attraction wdt:P131 wd:{wikidata_id}}}\n" for wikidata_id in nearby_city_wikidata_ids]
        delimiter = "\tUNION\n\t"
        sparql_query = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT DISTINCT ?attraction ?attractionLabel ?gps 

WHERE {{
    ?attraction (wdt:P31/wdt:P279*) wd:Q570116;
        wdt:P625 ?gps;
        rdfs:label ?attractionLabel.

    {delimiter.join(sparql_list)}   
    FILTER(LANG(?attractionLabel) = "en")
}}
        """
        logger.info("Getting nearby attractions")
        response = requests.get(
            f"https://query.wikidata.org/sparql?format=json&query={urllib.parse.quote_plus(sparql_query)}"
        )
        if response.ok:
            result = [attraction["attractionLabel"]["value"] for attraction in response.json()["results"]["bindings"]]
        else:
            result = NULL
        return list(set(result))

    def get_api_calls(self):
        return [self.get_nearby_tourist_attractions]

