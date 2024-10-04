from api_library.foursquare_api_client import FoursquareAPIClient
from constants.constants import NULL
from constants.enums.foursquare_field_enum import FoursquareField
from constants.enums.foursquare_sort_enum import FoursquareSortEnum


def attraction_in_city(
        attraction: str,
        city: str,
        state: str,
        name: bool,
        location: bool,
        categories: bool,
        distance: bool,
        email: bool,
        website: bool,
        rating: bool,
        popularity: bool,
        price: bool,
        exclude_chains: bool,
        number_of_results: int = 50,
) -> list[list[str]]:
    """
    Limits:
    {"number_of_results": [1, 50]}
    """
    foursquare_api_client = FoursquareAPIClient()
    list_of_attractions = foursquare_api_client.search(
        limit=number_of_results,
        query=attraction,
        sort=FoursquareSortEnum.RELEVANCE,
        name=name,
        location=location,
        categories=categories,
        distance=distance,
        email=email,
        website=website,
        rating=rating,
        popularity=popularity,
        price=price,
        near=f"{city}, {state}",
        exclude_all_chains=exclude_chains,
    )
    # create a sorted header row with the name field first
    all_fields = [
        name,
        location,
        categories,
        distance,
        email,
        website,
        rating,
        popularity,
        price,
    ]
    fields = [field_name for field_name, is_present in zip(FoursquareField.list(), all_fields) if is_present]
    header = [FoursquareField.NAME.value] + sorted([field.value for field in fields if field != FoursquareField.NAME])

    data = [header]

    if not list_of_attractions:
        data.append([NULL] * len(fields))
    else:
        for attraction in list_of_attractions:
            field_list = []
            for field in header:
                if FoursquareField(field) is FoursquareField.CATEGORIES:
                    value = ", ".join([category.get("name") for category in attraction.__dict__[field]])
                else:
                    value = attraction.__dict__[field]
                field_list.append(str(value))
            data.append(field_list)
    return data
