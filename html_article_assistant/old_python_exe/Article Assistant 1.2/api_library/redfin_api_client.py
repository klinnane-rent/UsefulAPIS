import datetime
import json
from enum import Enum

import requests
import us

from api_library.base_api_client import BaseAPIClient
from constants.constants import NULL
from constants.redfin_metric_mapping import REDFIN_METRIC_MAPPING
from constants.region_id_neighborhood_mapping import REGION_ID_NEIGHBORHOOD_MAPPING
from constants.region_id_place_mapping import REGION_ID_PLACE_MAPPING
from util.logger import get_logger

logger = get_logger(__name__)


class Tables(int, Enum):
    IDK = 32855


# region_type_id
class Regions(int, Enum):
    NEIGHBORHOOD = 1
    ZIPCODE = 2
    COUNTY = 5
    PLACE = 6


# property_type_id
class Property(int, Enum):
    CONDOMINIUM = 3
    MULTIFAMILY_2_TO_4_ = 4
    MULTIFAMILY_5_PLUS = 5
    SINGLE_FAMILY_HOME = 6
    RANCH = 7
    VACANT_LAND = 8
    TIMESHARE = 9
    TOWNHOUSE = 13


class RedfinAPIClient(BaseAPIClient):
    def __init__(self):
        super().__init__(api_key="data/keys/redfin_api_key.txt")
        self.endpoint = "https://4xq2salc9h.execute-api.us-west-2.amazonaws.com/prod/GetAggregates"

    def get_housing_data(self, metric: str, neighborhood, city: str, state: str):
        if neighborhood:
            # found_state = us.states.lookup(state)
            # if not found_state:
            #     logger.error(f"Unable to lookup state: '{state}'")
            #     result = None
            # else:
            #     state = found_state.abbr
            #     selected_metric = REDFIN_METRIC_MAPPING[metric]
            #     region_type_id = Regions.PLACE
            #     region_id = REGION_ID_PLACE_MAPPING.get(f"{city.title()}, {state}")
            #
            #     if not region_id:
            #         region_type_id = Regions.NEIGHBORHOOD
            #         region_id = REGION_ID_NEIGHBORHOOD_MAPPING.get(f"{city.title()}, {state}")
            #     if not region_id:
            #         return None

            if True:
                found_state = us.states.lookup(state)
                selected_metric = REDFIN_METRIC_MAPPING[metric]
                region_type_id = Regions.NEIGHBORHOOD
                region_id = REGION_ID_NEIGHBORHOOD_MAPPING.get(f"{neighborhood.title()}, {found_state.abbr}")

                params = {
                    "requests": [
                        {
                            "region_type_id": region_type_id,
                            "table_id": region_id,
                            "metric_name": selected_metric,
                        }
                    ]
                }
                response = requests.post(
                    url=self.endpoint,
                    headers={"x-api-key": self.key},
                    data=json.dumps(params)
                )
                response.raise_for_status()
                raw_results = response.json().get("aggregates")
                results = None if raw_results is None else raw_results[0]['data']
                if results is None or len(results) == 0:
                    result = NULL
                elif len(results) == 1:
                    result = float(list(results.values())[0])
                else:
                    now = datetime.datetime.now()
                    first = now.replace(day=1)
                    last_month = (first - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                    result = results.get(last_month, NULL)

                if result and metric.startswith("%"):
                    if result != NULL:
                        result = round(float(result) * 100, 4)
            logger.info(f"[{neighborhood}, {state}] {metric} = {result}")
            return result











        found_state = us.states.lookup(state)
        if not found_state:
            logger.error(f"Unable to lookup state: '{state}'")
            result = None
        else:
            state = found_state.abbr
            selected_metric = REDFIN_METRIC_MAPPING[metric]
            region_type_id = Regions.PLACE
            region_id = REGION_ID_PLACE_MAPPING.get(f"{city.title()}, {state}")

            if not region_id:
                region_type_id = Regions.NEIGHBORHOOD
                region_id = REGION_ID_NEIGHBORHOOD_MAPPING.get(f"{city.title()}, {state}")
            if not region_id:
                return None

            params = {
                "requests": [
                    {
                        "region_type_id": region_type_id,
                        "table_id": region_id,
                        "metric_name": selected_metric,
                    }
                ]
            }
            response = requests.post(
                url=self.endpoint,
                headers={"x-api-key": self.key},
                data=json.dumps(params)
            )
            response.raise_for_status()
            results = response.json()["aggregates"][0]["data"]
            if len(results) == 0:
                result = NULL
            elif len(results) == 1:
                result = float(list(results.values())[0])
            else:
                now = datetime.datetime.now()
                first = now.replace(day=1)
                last_month = (first - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                result = results.get(last_month, NULL)

            if result and metric.startswith("%"):
                if result != NULL:
                    result = round(float(result) * 100, 4)
        logger.info(f"[{city}, {state}] {metric} = {result}")
        return result

    def get_api_calls(self):
        return [self.get_housing_data]
