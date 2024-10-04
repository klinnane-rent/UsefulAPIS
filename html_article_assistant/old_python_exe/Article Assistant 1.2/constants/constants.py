from os import path

NULL = "No Data"

# If the 'city, state' combination does not exist in the FIPS mapping of the cities in the state provided by
# CensusAPIClient.get_city_fips_mapping(), add a mapping between the desired input and the actual value
# To find the actual value, put a breakpoint on the final result of the `possible_cities` variable
CITY_MAPPINGS_FIPS = {
    "louisville, ky": "louisville/jefferson county metro government"
}

ROOT_DIR = path.abspath(path.dirname(path.dirname(__file__)))
