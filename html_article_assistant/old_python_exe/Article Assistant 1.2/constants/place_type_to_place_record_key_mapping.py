from constants.enums.custom_article_place_type import CustomArticlePlaceType

PLACE_TYPE_TO_PLACE_RECORD_KEY_MAPPING = {
    f'{CustomArticlePlaceType.COUNTRY}': 'country_id',
    f'{CustomArticlePlaceType.STATE}': 'state_id',
    f'{CustomArticlePlaceType.COUNTY}': 'county_id',
    f'{CustomArticlePlaceType.CITY}': 'city_id',
    f'{CustomArticlePlaceType.NEIGHBORHOOD}': 'neighborhood_id',
}
