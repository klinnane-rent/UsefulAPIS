from flask import Flask, render_template, request, send_file, Response, jsonify
import csv
import io
from util.logger import get_logger

# processing data of tabs
import article_builders.attraction_in_city as cityAttraction
import article_builders.custom_article as customArticle
import article_builders.information_about_city  as cityInfo
import pandas as pd 
import numpy as np

import sys
sys.path.insert(0, '../')
import model.api as apiBuilder
import api_library.api_controller as apiController

logger = get_logger(__name__)
app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/custom-article', methods=['GET', 'POST'])
def custom_article():

    if request.method == 'POST':

        static_list = [ "All Median Rent By Month", 
                        "Condo/Co-op Median Rent By Month",
                        "Townhouse Median Rent By Month",
                        'Single Family Residential Median Rent By Month',
                        "Multi-Family (5+ Unit) Median Rent By Month",
                        "All MoM Median Rent Change",
                        "Condo/Co-op MoM Median Rent Change",
                        "Townhouse MoM Median Rent Change",
                        "Single Family Residential MoM Median Rent Change",
                        "Multi-Family (5+ Unit) MoM Median Rent Change"]

        data = {
            'is_topic_neighborhoods': request.form.get('topic_selection') == 'neighborhoods',
            'is_topic_cities': request.form.get('topic_selection') == 'cities',
            'is_topic_counties': request.form.get('topic_selection') == 'counties',
            'is_topic_states': request.form.get('topic_selection') == 'states',
            'is_citywide': request.form.get('width_selection') == 'citywide',
            'is_countywide': request.form.get('width_selection') == 'countywide',
            'is_statewide': request.form.get('width_selection') == 'statewide',
            'is_nationwide': request.form.get('width_selection') == 'nationwide',
            'neighborhood': request.form.get('neighborhood'),
            'city': request.form.get('city'),
            'county': request.form.get('county'),
            'state': request.form.get('state'),
            'search_radius': int(request.form.get('search_radius')),
            'min_population': int(request.form.get('min_population')),
            'max_population': int(request.form.get('max_population')),
            'number_of_results': int(request.form.get('number_of_results')),
            'methodologies': []
        }
        logger.info(data)
        if data['neighborhood'] == '':
            data['neighborhood'] = None
        api_clients = request.form.getlist('api_client[]')
        
        api_calls = request.form.getlist('api_call[]')
        weights = request.form.getlist('weight[]')
        arguments = request.form.getlist('arguments[]')
        sub_arguments = request.form.getlist('sub_arguments[]')
        orders = request.form.getlist('order[]')

        j = 0 
        for i in range(len(api_clients)):
            my_dict = { 'api_client': api_clients[i],
                'api_call': api_calls[i],
                'weight': float(weights[i]),
                'arguments': arguments[i] if arguments[i] != 'Disabled' else None,
                'order': orders[i] 
                }
            # manual targetting of sub arguments // front end processing code has bug.
            if len(sub_arguments) >= 0 and arguments[i] in static_list: 
                my_dict['arguments'] += ' ' + sub_arguments[j]
                j+=1
            
            data['methodologies'].append(my_dict)
        logger.info(data)
        # processes the data
        myApiCalls = parse_apis_and_methods(data['methodologies'])

        returned_results = customArticle.custom_article(data['is_topic_neighborhoods'], data['is_topic_cities'], data['is_topic_counties'], data['is_topic_states'], data['is_citywide'], data['is_countywide'], data['is_statewide'], data['is_nationwide'], data['neighborhood'], data['city'], data['county'], data['state'], data['search_radius'], data['min_population'], data['max_population'], data['number_of_results'],myApiCalls)
        
        if not returned_results:
            # If no results, send a JSON response to handle on the client side
            return jsonify({'status': 'no_results', 'message': 'There were no results.'}), 200

        columns = returned_results[0]  # Assuming these are the headers
        rows = returned_results[1:]    # Assuming these are the data rows

        df = pd.DataFrame(rows, columns=columns)
        csv_string = df.to_csv(index=False)

        return Response(
            csv_string,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment;filename=custom_article.csv"}
        )



    else:
        return render_template('custom_article.html')




@app.route('/city-info', methods=['GET', 'POST'])
def city_info():
    if request.method == 'POST':
        # Retrieve lists from the form
        api_clients = request.form.getlist('api_client[]')
        api_calls = request.form.getlist('api_call[]')
        weights = request.form.getlist('weight[]')
        arguments = request.form.getlist('arguments[]')

        city =  request.form.get('city')
        state =  request.form.get('state')
        # Prepare the data collection
        data = []

        print(arguments)
        for i in range(len(api_clients)):
            if api_clients[i] in ['TouristAPIClient', 'LocationsAPIClient', 'WalkscoreAPIClient']:
                arg = None
            else:
                arg = arguments.pop()
            entry = {
                'api_client': api_clients[i] ,
                'api_call': api_calls[i],
                'weight': float(weights[i]),
                'arguments': arg
            }
            data.append(entry)

        myApiCalls = parse_apis_and_methods(data)
        print(city)
        returned_results = cityInfo.information_about_city(city, state, myApiCalls)
        # Extracting column names
        columns = returned_results[0]

        # Extracting the data rows
        rows = returned_results[1:]

        # Creating the DataFrame
        df = pd.DataFrame(rows, columns=columns)

       
        # Convert DataFrame to CSV string
        csv_string = df.to_csv(index=False)
        # Create a response
        return Response(
            csv_string,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment;filename=information_about_{city}.csv"}
        )
    else:
        # If it's not a POST request, show the form
        return render_template('city_info.html')


@app.route('/attractions', methods=['GET', 'POST'])
def attractions():
    if request.method == 'POST':
        # Extracting and processing all possible form data:
       
        data = {
            'attraction': request.form.get('attraction'),
            'city': request.form.get('city'),
            'state': request.form.get('state'),
            'include_name': 'name' in request.form,
            'include_location': 'location' in request.form,
            'include_categories': 'categories' in request.form,
            'include_distance': 'distance' in request.form,
            'include_email': 'email' in request.form,
            'include_website': 'website' in request.form,
            'include_rating': 'rating' in request.form,
            'include_popularity': 'popularity' in request.form,
            'include_price': 'price' in request.form,
            'exclude_chains': 'exclude_chains' in request.form,
            'number_of_results': int(request.form.get('number_of_results'))
        }
        logger.info(data)
        returned_results = cityAttraction.attraction_in_city(data['attraction'], data['city'], data['state'], data['include_name'], data['include_location'], data['include_categories'], data['include_distance'], data['include_email'], data['include_website'], data['include_rating'], data['include_popularity'], data['include_price'], data['exclude_chains'], data['number_of_results'])

        # Extracting column names
        columns = returned_results[0]

        # Extracting the data rows
        rows = returned_results[1:]

        # Creating the DataFrame
        df = pd.DataFrame(rows, columns=columns)

        df.to_csv('results.csv')
        # Convert DataFrame to CSV string
        csv_string = df.to_csv(index=False)
        # Create a response
        return Response(
            csv_string,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment;filename=attraction_in_{data['city']}.csv"}
        )
        

    return render_template('attractions.html')
def pop_up_empty_results():
    print('here')
    return jsonify({'error': 'Please add at least one row in the Methodologies section.'}), 400


def parse_apis_and_methods(data, order = False):
   
    
    process = apiController.APIProcessor()
    for idx, d in enumerate(data):
        if 'order' in d.keys():
            order = d['order']
        process.add_row(api_client = d['api_client'])
        process.update_row(idx, d['api_client'], d['api_call'], d['weight'], d['arguments'],order)
    return process.get_values()



if __name__ == '__main__':
    app.run(debug=True)
