import csv
from os import path

from constants.constants import ROOT_DIR

neighborhood_data = dict()

places_keydict = {
    "neighborhood_id": 0,
    "neighborhood": 1,
    "city_id": 2,
    "city": 3,
    "county_id": 4,
    "county": 5,
    "state_id": 6,
    "state": 7,
    "postal_abbr": 8,
}

# Read neighborhood data from CSV file
with open(path.join(ROOT_DIR, "hood-city-county-state.csv"), encoding="utf-8") as file:
    reader = csv.reader(file)
    reader.__next__()
    for i, row in enumerate(reader):
        neighborhood_data[row[places_keydict["neighborhood"]] + ", " + row[places_keydict["postal_abbr"]]] = {
            'neighborhood_id': row[places_keydict['neighborhood_id']],
            'neighborhood'   : row[places_keydict['neighborhood']],
            'city_id'        : row[places_keydict['city_id']],
            'city'           : row[places_keydict['city']],
            'county_id'      : row[places_keydict['county_id']],
            'county'         : row[places_keydict['county']],
            'state_id'       : row[places_keydict['state_id']],
            'state'          : row[places_keydict['state']],
            'postal_abbr'    : row[places_keydict['postal_abbr']],
        }

# Write neighborhood data as a dictionary to a python file
with open(path.join(ROOT_DIR, "constants/name_to_neighborhood_info.py"), mode="w+", encoding="utf-8") as file:
    file.write("NEIGHBORHOOD_STATE_TO_FULL_DETAIL_MAPPINGS = {\n")
    for key, value in neighborhood_data.items():
        file.write(f'    "{key}": {value},\n')
    file.write("}\n")
