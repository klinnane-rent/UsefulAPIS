
from serpapi import GoogleSearch
from openpyxl import Workbook
import os
import time
import json
params = {
    "engine": "google",
    "q": "",
    "location": "United States",
    "google_domain": "google.com",
    "gl": "us",
    "hl": "en",
    "device": "mobile",
    "api_key": ""
}

# Creating a new workbook
workbook = Workbook()

# Creating a new sheet in workbook
results_sheet = workbook.active
results_sheet.title = "Organic Search Results"

# Adding headers to the Excel file
results_headers = ["Keyword", "URL", "Rank"]
results_sheet.append(results_headers)

# Opening the txt file and reading the cities within the sheet
print('here')
queries_file = "suberbs.txt"
with open(queries_file, "r") as queries:

    # iterating through the locations list and extracting search results for each city
    for query in queries:
        # Printing query, so I know here I am in the loop.
        # Extracting Search Results
        # Note you will need to combine [Location] and [query type] together before you run this so 
        params["q"] = query
        search = GoogleSearch(params)
        results = search.get_dict()
        try:
            #formatted_results = {}
            count_of_branded = 0 
            print("\n")
            print(results['organic_results'])
            print(results.keys())
            
            if 'organic_results' in results.keys():
                is_in = False
                for result in results['organic_results']:
                    
                    row = [query, result['link'],result['position']]
                    results_sheet.append(row) 

                
            else:
                results_sheet.append([query,'NO ORGANIC']) 
                    
                #json_object = json.dumps(formatted_results, indent = 4) 
                #row = [query,  json_object]
                #results_sheet.append(row)
                time.sleep(.5)
        
        except Exception as e:
            results_sheet.append([query,'ERROR']) 
            print(e)
            workbook.save("results.xlsx")
        workbook.save("results.xlsx")
# Saving the workbook
workbook.save("results.xlsx")
