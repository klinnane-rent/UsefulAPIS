import json


"""
Reads a CSV file with a schema of "city,state,region_id,region_type_id,is_valid" and creates a Python dictionary in
another file as source code. May need to check the contents of the CSV to ensure there is no case like:
Seattle,Wa,WA,12345,6,TRUE

Special characters like in Peña Blanca will be in the dictionary as Pe\u00f1a Blanca due to how PyCharm does encoding
"""

csv_file = "C:\\Users\\jenna.hall\\PycharmProjects\\WordsmithProject\\data\\Cities + Region Ids - places data.csv"
dictionary_file = "/constants/region_id_place_mapping.py"

with open(csv_file, encoding="utf-8") as file:
    x = file.readlines()[1:]

with open(dictionary_file, "w") as file:
    file.write("REGION_ID_MAPPING = ")
    mapping = dict()
    for line in x:
        city, state, region_id, region_type_id, is_valid, *rest = line.split(",")
        if bool(is_valid) and int(region_type_id) == 6:
            mapping[f"{city}, {state}"] = str(region_id)
    file.write(json.dumps(mapping, indent=4))
