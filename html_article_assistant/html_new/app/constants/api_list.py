from api_library.census_api_client import CensusAPIClient
from api_library.foursquare_api_client import FoursquareAPIClient
from api_library.geo_database_client import LocationsAPIClient
from api_library.google_api_client import GoogleMapsAPIClient
from api_library.redfin_api_client import RedfinAPIClient
from api_library.tourist_api_client import TouristAPIClient
from api_library.walkscore_api_client import WalkscoreAPIClient

API_CLIENT_LIST = [
    GoogleMapsAPIClient,
    WalkscoreAPIClient,
    FoursquareAPIClient,
    LocationsAPIClient,
    CensusAPIClient,
    RedfinAPIClient,
    TouristAPIClient,
]
