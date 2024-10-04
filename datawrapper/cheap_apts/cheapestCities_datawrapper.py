import pandas as pd

# import data. these files are from Mason 
cheap = pd.read_csv('Cheapest_Places_Tranformed_Data.csv')
top = pd.read_csv('Timeseries_Top_5_Cities.csv')


# below are functions to manipulate data into right format for datawrapper
# each were written to take in one state at a time

# data table function (for bottom of page)
def tableCheapest(data, state, save=True):
    
    # data = cheap, state = 'GA', etc

    tableCols = ['city', 'state', 'total_pop', 'median_income', 
                 'owner_occupied_housing_units_median_value', 
                 '2_rent', 'score']
    
    data_ = data[tableCols]
    data_.loc[:, 'score'] = data_.score * 100
    
    out = data_[data_.state == state]
    out.sort_values(by='score', ascending=False, inplace=True)
    
    nameD = {'city': 'City, State',
             'total_pop': 'Population',
             'median_income': 'Median Income',
             'owner_occupied_housing_units_median_value': 'Median Home Value',
             '2_rent': '2-Bed Asking Rent',
             'score': 'Score'
            }
    
    out = out[[k for k in nameD.keys()]]
    out.rename(columns=nameD, inplace=True)
    out.dropna(inplace=True)
    
    if save:
        out.to_csv(f'states/{state}/Table/data.csv', index=False)
        
    return out

# map function
def mapCheapest(data, state, save=True):
    
    # data = output from tableCheapest(), state = 'GA', etc
    
    latlong = pd.read_csv('Cheapest_Places_Tranformed_Data.csv')
    latlong = latlong[['city', 'census_geo_id', 'id', 'geo_id', 'shape_id', 'lat', 'lon']]
    latlong.rename(columns={'city': 'City, State'}, inplace=True)
    
    get = [c for c in data['City, State'][:5]]
    top = latlong[latlong['City, State'].isin(get)]
    
    out = data.merge(top, on='City, State')
    
    if save:
        out.to_csv(f'states/{state}/Map/data.csv', index=False)
        
    return out

# timeseries function
def timeseriesCheapest(data, state, save=True):
    
    # data = top, state = 'GA', etc
    
    df = data[data.state == state].copy()
    out = df.pivot(index='date', columns='city', values='avg_rent')
    
    if save:
        out.to_csv(f'states/{state}/Timeseries/data.csv')
    
    return out


# use sequence below to generate all inputs for datawrapper
# below is only Georgia 
state_abs = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 
    'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 
    'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]


for i, state_ab in enumerate(state_abs):
    print(f"{i}\t:\t{state_ab}")
    gaCheap = tableCheapest(cheap, state_ab, True)
    gaMap = mapCheapest(gaCheap, state_ab, True)
    gaTime = timeseriesCheapest(top, state_ab, True)

# filenames for outputs:
# table = 'cheapestCities_{state}_table.csv'
# map = 'cheapestCities_{state}_map.csv'
# timeseries = 'cheapestCites_{state}_timeseries.csv'