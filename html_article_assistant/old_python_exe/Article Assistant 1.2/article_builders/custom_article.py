import concurrent.futures


from api_library.census_api_client import CensusAPIClient
from api_library.foursquare_api_client import FoursquareAPIClient
from api_library.google_api_client import GoogleMapsAPIClient
from api_library.redfin_api_client import RedfinAPIClient
from api_library.walkscore_api_client import WalkscoreAPIClient
from constants.constants import NULL
from constants.enums.custom_article_place_type import CustomArticlePlaceType
from constants.name_to_neighborhood_info import NEIGHBORHOOD_STATE_TO_FULL_DETAIL_MAPPINGS
from constants.place_type_to_place_record_key_mapping import PLACE_TYPE_TO_PLACE_RECORD_KEY_MAPPING
from model.address import Address
from model.api import API
from model.ranking_table import Table, RankedItem
from util.logger import get_logger
import csv


logger = get_logger(__name__)




def multithreaded_api_call(func, args):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(args)) as executor:
        futures = []
        for arg in args:
            futures.append(executor.submit(func, arg))
        executor.shutdown(False)
        for future in futures:
            results.append(future.result())
    return results


def custom_article(
        is_topic_neighborhoods: bool = False,
        is_topic_cities: bool = False,
        is_topic_counties: bool = False,
        is_topic_states: bool = False,
        is_citywide: bool = False,
        is_countywide: bool = False,
        is_statewide: bool = False,
        is_nationwide: bool = False,
        neighborhood: str = None,
        city: str = None,
        county: str = None,
        state: str = None,
        search_radius_in_miles: int = 50,
        minimum_population: int = 20000,
        maximum_population: int = 99999999,
        number_of_results: int = 25,
        methodologies: list[API] = None,
) -> list[list]:
    """
    Limits:
    {"search_radius_in_miles": [0, 500], "number_of_results": [1, 100], "minimum_population": [10, 10000000], "maximum_population": [1, 99999999]}
    """
    # Avoid circular import
    from api_library.geo_database_client import LocationsAPIClient

    # define what the article is about, if stated
    topic = None

    if is_topic_neighborhoods:
        topic = CustomArticlePlaceType.NEIGHBORHOOD
    elif is_topic_cities:
        topic = CustomArticlePlaceType.CITY
    elif is_topic_counties:
        topic = CustomArticlePlaceType.COUNTY
    elif is_topic_states:
        topic = CustomArticlePlaceType.STATE

    # define what the scope of the article domain is, if stated
    scope = None

    if is_citywide:
        scope = CustomArticlePlaceType.CITY
    elif is_countywide:
        scope = CustomArticlePlaceType.COUNTY
    elif is_statewide:
        scope = CustomArticlePlaceType.STATE
    elif is_nationwide:
        scope = CustomArticlePlaceType.COUNTRY

    # Get city/neighborhood
    location = f"{city.title()}, {state.upper()}"
    lat, lon = GoogleMapsAPIClient().address_to_coordinates(location)
    locations_client = LocationsAPIClient()
    city_state = locations_client.get_city_from_coordinates(lat, lon, location)

    if city_state is None:
        logger.error(f"Unable to generate custom article for location: '{location}'")
        ranked_table = [["Bad Location"] + [NULL] * len(methodologies)]
        for methodology in methodologies:
            if methodology.client == FoursquareAPIClient:
                methodology.column_name = methodology.column_name.replace("Places", methodology.args.title())
            elif methodology.client == LocationsAPIClient:
                methodology.column_name = "Population"
            elif methodology.client == CensusAPIClient:
                methodology.column_name = methodology.args
            elif methodology.client == RedfinAPIClient:
                methodology.column_name = methodology.args
    else:
        if scope is not None and topic is not None:
            print('TOPIC! SCOPE!')
            # gather the places of specified type within the region of specified scope
            return write_custom_article_region_based(topic, scope, neighborhood, city, county, state, minimum_population, maximum_population, number_of_results, methodologies)
        elif scope is not None or topic is not None:
            # inform the user that both must be defined
            pass
        else:
            print('NOTHING! FOUND! OLD WAY!')
            # do radius-based
            logger.info(f"Looking for all neighborhoods/cities within {search_radius_in_miles} miles of {city_state}")
            logger.info("Quitting because nationwide are not implemented yet")
            return write_custom_article_radius_based(city_state, search_radius_in_miles, minimum_population, maximum_population, number_of_results, methodologies)


def get_cleaned_county_input(raw_county_input):
    county_comparator = raw_county_input[-6:].title()
    is_county_in_input = len(raw_county_input) > 4 and county_comparator == 'County'
    cleaned_county_input = raw_county_input if is_county_in_input is True else f'{raw_county_input} County'
    return cleaned_county_input


def write_custom_article_region_based(
    topic,
    scope,
    neighborhood_input,
    city_input,
    county_input,
    state_input,
    minimum_population: int,
    maximum_population: int,
    number_of_results: int,
    methodologies: list[API],
) -> list[list]:
    country_input = "United States of America"

    # Avoid circular import
    from api_library.geo_database_client import LocationsAPIClient

    county_input = get_cleaned_county_input(county_input)

    # validate topic and scope are compatible/valid
    if topic == CustomArticlePlaceType.NEIGHBORHOOD:
        # all scopes valid
        pass
    elif topic == CustomArticlePlaceType.CITY and scope == CustomArticlePlaceType.CITY:
        # only one result. invalid
        raise Exception('scope and topic combo restrict article subject to one entity.')
    elif topic == CustomArticlePlaceType.COUNTY and scope == CustomArticlePlaceType.COUNTY:
        # only one result. invalid
        raise Exception('scope and topic combo restrict article subject to one entity.')

    # if topic == CustomArticlePlaceType.COUNTY:
    #     raise Exception('counties not implemented yet')

    place_scope_match_predicate = get_place_scope_match_predicate(scope, neighborhood_input, city_input, county_input, state_input, country_input)

    logger.info(f"Looking for all {topic} in the {scope} of {place_scope_match_predicate}")

    article_places = get_place_records(scope, topic, neighborhood_input, city_input, county_input, state_input, country_input)

    if topic == CustomArticlePlaceType.CITY:
        locations_client = LocationsAPIClient()

        for place in article_places:
            location = f'{place["city"]}, {place["state"]}'
            lat, lon = GoogleMapsAPIClient().address_to_coordinates(location)
            place['lat'] = lat
            place['lon'] = lon
            location_info = locations_client.get_city_from_coordinates(lat, lon, location)
            # print(location_info)
            place['population'] = location_info['population'] if location_info is not None else 0

    print(article_places)

    # Scale weights to sum to 100
    weights = [api_call.weight for api_call in methodologies]
    if sum(weights) != 1:
        scaling_factor = 1 / sum(weights)
        for i, methodology in enumerate(methodologies):
            methodology.weight *= scaling_factor

    init_items = []
    if topic == CustomArticlePlaceType.NEIGHBORHOOD:
        init_items = [f'{place["neighborhood"]}, {place["city"]}, {place["state"]}' for place in article_places]
    elif topic == CustomArticlePlaceType.CITY:
        init_items = [f'{place["city"]}, {place["state"]}' for place in article_places]
    elif topic == CustomArticlePlaceType.COUNTY:
        init_items = [f'{place["county"]}, {place["state"]}' for place in article_places]
    else:
        init_items = [f'{place["state"]}' for place in article_places]

    table = Table(initial_items=init_items)
    for i, methodology in enumerate(methodologies):
        if methodology.client == WalkscoreAPIClient:
            args = []
            if topic == CustomArticlePlaceType.NEIGHBORHOOD:
                args = [{"address": f'{place["neighborhood"]}, {place["city"]}, {place["state"]}'} for place in article_places]
            elif topic == CustomArticlePlaceType.CITY:
                args = [{"address": f'{place["city"]}, {place["state"]}'} for place in article_places]
            elif topic == CustomArticlePlaceType.COUNTY:
                args = [{"address": f'{place["county"]}, {place["state"]}'} for place in article_places]
            else:
                args = [{"address": f'{place["state"]}'} for place in article_places]

            print('places')
            results = multithreaded_api_call(methodology.call_api, args)
            print(results)
        elif methodology.client == GoogleMapsAPIClient:
            place_name = None
            destinations = [f"{place[f'{topic}']}, {place['state']}" for place in article_places]

            if topic == CustomArticlePlaceType.NEIGHBORHOOD:
                place_name = neighborhood_input
            elif topic == CustomArticlePlaceType.CITY:
                place_name = city_input
            elif topic == CustomArticlePlaceType.COUNTY:
                place_name = county_input
            else:
                destinations = [f"{place['name']}, {place['region']}" for place in article_places]

            results = methodology.call_api(
                {"location": f"{place_name}, {state_input}", "destinations": destinations}
            )
            methodology.column_name = methodology.column_name.replace(
                "Location", f"{place_name}, {state_input}"
            )
        elif methodology.client == FoursquareAPIClient:
            args = [
                {"ll": f"{place['latitude']},{place['longitude']}", "query": methodology.args} for place in article_places
            ]
            results = multithreaded_api_call(methodology.call_api, args)
            methodology.column_name = methodology.column_name.replace("Places", methodology.args.title())
        elif methodology.client == LocationsAPIClient:
            if topic != CustomArticlePlaceType.CITY:
                raise Exception('GeoDB not usable for population on a scale other than cities')
            results = [place["population"] for place in article_places]
            methodology.column_name = "Population"
        elif methodology.client == CensusAPIClient:
            methodology.column_name = methodology.args
            results_dict = dict()
            for place in article_places:
                state = place["region"]
                city_name = place["name"]
                if state in results_dict:
                    results_dict[state][city_name] = 0
                else:
                    results_dict[state] = dict()
                    results_dict[state][city_name] = 0
            for state in results_dict.keys():
                cities = list(results_dict[state].keys())
                args = {"cities": cities, "state": state, "selection": methodology.args}
                state_results = methodology.call_api(args)
                for idx, city in enumerate(cities):
                    results_dict[state][city] = state_results[idx]
            results = []
            for place in article_places:
                place_name = place["name"]
                state = place["region"]
                results.append(results_dict[state][place_name])
        elif methodology.client == RedfinAPIClient:
            args = [
                {"neighborhood": place["neighborhood"], "city": None, "state": place["state"], "metric": methodology.args} for place in article_places
            ]
            results = multithreaded_api_call(methodology.call_api, args)
            methodology.column_name = methodology.args
            for index, result in enumerate(results):
                if result is None:
                    results[index] = 0
        else:
            results = methodology.call_api('')


        # items = [
        #     RankedItem(f"{article_places[i]['name']}, {article_places[i]['region']}", value)
        #     for i, value in enumerate(results)
        # ]

        enumerated_results = enumerate(results)

        items = []

        if topic == CustomArticlePlaceType.NEIGHBORHOOD:
            items = [
                RankedItem(
                    f"{article_places[i]['neighborhood']}, {article_places[i]['city']}, {article_places[i]['state']}",
                    value)
                for i, value in enumerate(results)
            ]
        elif topic == CustomArticlePlaceType.CITY:
            items = [
                RankedItem(
                    f"{article_places[i]['city']}, {article_places[i]['state']}",
                    value)
                for i, value in enumerate(results)
            ]
        elif topic == CustomArticlePlaceType.COUNTY:
            items = [
                RankedItem(
                    f"{article_places[i]['county']}, {article_places[i]['state']}",
                    value)
                for i, value in enumerate(results)
            ]
        else:
            items = [
                RankedItem(
                    f"{article_places[i]['state']}",
                    value)
                for i, value in enumerate(results)
            ]

        table.add_column(
            methodology.column_name,
            methodology.weight,
            items,
            methodology.descending
        )

    row_numbers_to_remove = []

    for col in table.columns:
        i_row = 0
        if col.name == 'Population':
            for row in col.items:
                if row.value < minimum_population or row.value > maximum_population:
                    row_numbers_to_remove.append(i_row)
                i_row = i_row + 1

    # remove cities with bad populations
    while len(row_numbers_to_remove) > 0:
        row_being_deleted = row_numbers_to_remove.pop()
        for col in table.columns:
            col.items.pop(row_being_deleted)
        table.items.pop(row_being_deleted)

    ranked_table = table.rank()

    return_stuff = [[f"{topic.title()}"] + [methodology.column_name for methodology in methodologies]] + ranked_table

    return return_stuff

def write_custom_article_radius_based(
    city_state,
    search_radius_in_miles: int,
    minimum_population: int,
    maximum_population: int,
    number_of_results: int,
    methodologies: list[API],
) -> list[list]:
    # Avoid circular import
    from api_library.geo_database_client import LocationsAPIClient

    client = LocationsAPIClient()

    buffer_to_ensure_enough_cities = 5
    number_of_results = number_of_results + buffer_to_ensure_enough_cities

    nearby_cities = client.get_nearby_cities(
        city_id=city_state["wikiDataId"],
        radius=search_radius_in_miles,
        min_population=minimum_population,
        max_population=maximum_population,
        limit=number_of_results,
    )
    print(nearby_cities)
    # quit()
    # except (KeyError, TypeError):
    #     logger.warn("Getting nearby neighborhoods from Google Maps Places API")
    #     nearby_cities = GoogleMapsAPIClient().get_nearby_neighborhoods((lat, lon), search_radius_in_miles * 1609)

    # Scale weights to sum to 100
    weights = [api_call.weight for api_call in methodologies]
    if sum(weights) != 1:
        scaling_factor = 1 / sum(weights)
        for i, methodology in enumerate(methodologies):
            methodology.weight *= scaling_factor

    table = Table(initial_items=[f"{city['name']}, {city['region']}" for city in nearby_cities])
    for i, methodology in enumerate(methodologies):
        if methodology.client == WalkscoreAPIClient:
            args = [{"address": f'{city["name"]}, {city["region"]}'} for city in nearby_cities]
            results = multithreaded_api_call(methodology.call_api, args)
        elif methodology.client == GoogleMapsAPIClient:
            destinations = [f"{city['name']}, {city['region']}" for city in nearby_cities]
            results = methodology.call_api(
                {"location": f"{city_state['name']}, {city_state['region']}", "destinations": destinations}
            )
            methodology.column_name = methodology.column_name.replace(
                "Location", f"{city_state['name']}, {city_state['region']}"
            )
        elif methodology.client == FoursquareAPIClient:
            args = [
                {"ll": f"{city['latitude']},{city['longitude']}", "query": methodology.args} for city in nearby_cities
            ]
            results = multithreaded_api_call(methodology.call_api, args)
            methodology.column_name = methodology.column_name.replace("Places", methodology.args.title())
        elif methodology.client == LocationsAPIClient:
            results = [city["population"] for city in nearby_cities]
            methodology.column_name = "Population"
        elif methodology.client == CensusAPIClient:
            methodology.column_name = methodology.args
            results_dict = dict()
            for city in nearby_cities:
                state = city["region"]
                city_name = city["name"]
                if state in results_dict:
                    results_dict[state][city_name] = 0
                else:
                    results_dict[state] = dict()
                    results_dict[state][city_name] = 0
            for state in results_dict.keys():
                cities = list(results_dict[state].keys())
                args = {"cities": cities, "state": state, "selection": methodology.args}
                state_results = methodology.call_api(args)
                for idx, city in enumerate(cities):
                    results_dict[state][city] = state_results[idx]
            results = []
            for city in nearby_cities:
                city_name = city["name"]
                state = city["region"]
                results.append(results_dict[state][city_name])
        elif methodology.client == RedfinAPIClient:
            args = [
                {"neighborhood": None, "city": city["name"], "state": city["region"], "metric": methodology.args} for city in nearby_cities
            ]
            results = multithreaded_api_call(methodology.call_api, args)
            methodology.column_name = methodology.args
            for index, result in enumerate(results):
                if result is None:
                    results[index] = 0
        else:
            results = methodology.call_api('')

        items = [
            RankedItem(f"{nearby_cities[i]['name']}, {nearby_cities[i]['region']}", value)
            for i, value in enumerate(results)
        ]
        table.add_column(
            methodology.column_name,
            methodology.weight,
            items,
            methodology.descending
        )

    row_numbers_to_remove = []

    for col in table.columns:
        i_row = 0
        if col.name == 'Population':
            for row in col.items:
                if row.value < minimum_population or row.value > maximum_population:
                    row_numbers_to_remove.append(i_row)
                i_row = i_row + 1

    print(row_numbers_to_remove)

    # remove cities with bad populations
    while len(row_numbers_to_remove) > 0:
        row_being_deleted = row_numbers_to_remove.pop()
        for col in table.columns:
            col.items.pop(row_being_deleted)
        table.items.pop(row_being_deleted)

    ranked_table = table.rank()

    return [["City"] + [methodology.column_name for methodology in methodologies]] + ranked_table


def get_place_of_article_type(topic, neighborhood, city, county, state, country):
    if topic == CustomArticlePlaceType.NEIGHBORHOOD:
        if neighborhood is None:
            raise Exception(f"Article type {CustomArticlePlaceType.NEIGHBORHOOD}, but no {CustomArticlePlaceType.NEIGHBORHOOD} was specified")
        return neighborhood
    if topic == CustomArticlePlaceType.CITY:
        if city is None:
            raise Exception(f"Article type {CustomArticlePlaceType.CITY}, but no {CustomArticlePlaceType.CITY} was specified")
        return city
    if topic == CustomArticlePlaceType.COUNTY:
        if county is None:
            raise Exception(f"Article type {CustomArticlePlaceType.COUNTY}, but no {CustomArticlePlaceType.COUNTY} was specified")
        return county
    if topic == CustomArticlePlaceType.STATE:
        if state is None:
            raise Exception(f"Article type {CustomArticlePlaceType.STATE}, but no {CustomArticlePlaceType.STATE} was specified")
        return state
    if topic == CustomArticlePlaceType.COUNTRY:
        raise Exception(f"{CustomArticlePlaceType.COUNTRY}-focused articles not yet supported")


def get_place_scope_match_predicate(scope, neighborhood, city, county, state, country):
    if scope == CustomArticlePlaceType.NEIGHBORHOOD:
        raise Exception(
                f"Scope type {CustomArticlePlaceType.NEIGHBORHOOD} is too small")
    if scope == CustomArticlePlaceType.CITY:
        if city is None:
            raise Exception(
                f"Article scope {CustomArticlePlaceType.CITY}, but no {CustomArticlePlaceType.CITY} was specified")
        return city
    if scope == CustomArticlePlaceType.COUNTY:
        if county is None:
            raise Exception(
                f"Article scope {CustomArticlePlaceType.COUNTY}, but no {CustomArticlePlaceType.COUNTY} was specified")
        return county
    if scope == CustomArticlePlaceType.STATE:
        if state is None:
            raise Exception(
                f"Article scope {CustomArticlePlaceType.STATE}, but no {CustomArticlePlaceType.STATE} was specified")
        return state
    if scope == CustomArticlePlaceType.COUNTRY:
        if country is None:
            raise Exception(
                f"Article scope {CustomArticlePlaceType.COUNTRY}, but no {CustomArticlePlaceType.COUNTRY} was specified")
        return country


def get_place_records(scope, topic, neighborhood, city, county, state, country):
    records_to_filter = list(NEIGHBORHOOD_STATE_TO_FULL_DETAIL_MAPPINGS.values())
    filtered_records = []
    already_added = dict()

    # TODO: make sure topic is smaller place type than scope

    place_input_of_specified_article_type = get_place_of_article_type(topic, neighborhood, city, county, state, country)
    place_scope_match_predicate = get_place_scope_match_predicate(scope, neighborhood, city, county, state, country)
    scope_ref = 'postal_abbr' if str(scope) == 'state' and len(state) == 2 else str(scope)
    if scope_ref == 'postal_abbr':
        place_scope_match_predicate = place_scope_match_predicate.upper()
    place_record_predicate_id_type = PLACE_TYPE_TO_PLACE_RECORD_KEY_MAPPING.get(str(topic))

    logger.info(f"Looking for all {topic} records within the {scope} of {place_scope_match_predicate}")

    for place_record in records_to_filter:
        id_of_article_place_type = place_record.get(place_record_predicate_id_type)
        name_of_record_place_of_specified_type = place_record.get(str(topic))
        name_of_record_place_of_specified_scope = place_record.get(scope_ref)

        if (
                place_scope_match_predicate.lower() == name_of_record_place_of_specified_scope.lower()
                and id_of_article_place_type is not None
                and already_added.get(id_of_article_place_type) is not True
        ):
            already_added[id_of_article_place_type] = True
            filtered_records.append(place_record)

    return filtered_records
