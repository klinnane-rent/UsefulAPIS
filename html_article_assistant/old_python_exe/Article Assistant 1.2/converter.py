import csv
from os import path

from constants.constants import ROOT_DIR

neighborhood_data = dict()

# Neighborhood ID,Neighborhood Name,City Name,State,

# Read neighborhood data from CSV file
with open(path.join(ROOT_DIR, "neighborhood.csv"), encoding="utf-8") as file:
    reader = csv.reader(file)
    reader.__next__()
    for i, row in enumerate(reader):
        if row[3] != "#N/A":
            neighborhood_data[row[1] + ", " + row[3]] = row[0]

# Write neighborhood data as a dictionary to a python file
with open(path.join(ROOT_DIR, "constants/region_id_neighborhood_mapping.py"), mode="w+", encoding="utf-8") as file:
    file.write("REGION_ID_NEIGHBORHOOD_MAPPING = {\n")
    for key, value in neighborhood_data.items():
        file.write(f'    "{key}": "{value}",\n')
    file.write("}\n")
