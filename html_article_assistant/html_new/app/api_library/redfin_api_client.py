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
from datetime import timedelta

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
        

    def get_housing_data(self, metric: str, neighborhood, city: str, state: str):
        self.endpoint = "https://4xq2salc9h.execute-api.us-west-2.amazonaws.com/prod/GetAggregates"
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
                logger.debug(region_id)
                logger.debug(region_type_id)
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
    def get_rental_data(self, metric: str, neighborhood, city: str, state: str):

        logger.debug(metric)
        logger.debug(neighborhood)
        logger.debug(city)
        logger.debug(state)
        if ' Median Rent By Month' in metric: 
            selected_metric = 'value'
            ptype = metric.split(' Median Rent By Month')[0]
            time_period = metric.split(' Median Rent By Month ')[1]
        else: 
            selected_metric = 'mom' 
            ptype = metric.split(' MoM Median Rent Change')[0]
            time_period =  metric.split(' MoM Median Rent Change ')[1]
        
        self.endpoint = "https://www.redfin.com/stingray/api/rentalMarketTrends/6/"
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
                header = {'User-Agent': 'seoradar'}
                
                response = requests.get(
                    url=self.endpoint,
                    headers={"x-api-key": self.key},
                    
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
            #selected_metric = REDFIN_METRIC_MAPPING[metric]
            region_type_id = Regions.PLACE
            region_id = REGION_ID_PLACE_MAPPING.get(f"{city.title()}, {state}")
            
            if not region_id:
                region_type_id = Regions.NEIGHBORHOOD
                region_id = REGION_ID_NEIGHBORHOOD_MAPPING.get(f"{city.title()}, {state}")
            if not region_id:
                return None
            self.endpoint = "https://www.redfin.com/stingray/api/rentalMarketTrends/6/" + str(region_id)
            header = {'User-Agent': 'seoradar', "x-api-key": self.key}
            response = requests.get(
                url=self.endpoint,
                headers=header
            )
            response.raise_for_status()

            if response.text.startswith('{}&&'):
                clean_json = response.text[4:]  # Remove the first 4 characters
            else:
                clean_json = response.text

            # Parse the JSON data
            try:
                data = json.loads(clean_json)
                
            except json.JSONDecodeError as e:
                print("Failed to decode JSON:", e)
  
            property_types = data['payload']['medianRentByMonthAndPropertyType']
            data_dict = {}
            locals
            for val in property_types:
               
                if 'propertyType' in val.keys():
                    if  ptype == val['propertyType']:
                        for monthlyVal in val['medianRentByMonth']:
                            
                            if selected_metric in monthlyVal.keys():
                                
                                allDatesValue = monthlyVal[selected_metric]
                                myDate = monthlyVal['date']
                                data_dict[datetime.datetime.strptime(myDate, "%Y-%m-%d")] = allDatesValue
            #print(data_dict)
            sorted_dates = sorted(data_dict.keys())
            most_recent_date = sorted_dates[-1]
            
            # Define periods
            periods = {
                "last_1_month": 1,  
                "last_3_month": 3,
                "last_6_month": 6,
                "last_12_month": 12
            }

            # Calculate values
            results = {}
            for key, months in periods.items():
                target_date = most_recent_date - timedelta(days=30 * months)
                # Find the closest earlier or exact date
                closest_date = max((date for date in sorted_dates if date <= target_date), default=None)
                if closest_date:
                    results[key] = data_dict[closest_date]
                else:
                    results[key] = None  # or "N/A" if no data is available for that period

        try:
            result = float(results[time_period])
        except KeyError:
            result = None
        logger.info(f"[{city}, {state}] {metric} = {result}")
        return result

    def get_api_calls(self):
        return [self.get_housing_data, self.get_rental_data]
