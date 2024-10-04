import pandas as pd 
import json
import numpy as np
import re
from os import path


class page_rules():
    ag, rent, rentals = None, None, None
    def __init__(self):
        path_prefix = path.dirname(path.abspath(__file__))
        rent_path = path.join(path_prefix, 'regex/rent_regex.json')
        ag_path = path.join(path_prefix, 'regex/ag_regex.json')
        rentals_path = path.join(path_prefix, 'regex/rentals_regex.json')
        self.ag,self.rent,self.rentals = json.load(open(ag_path,'r') ), json.load(open(rent_path,'r') ),json.load(open(rentals_path,'r') )
        
    
    import re
    def create_labels(self, x,site='ag'):
        regex_list = None 
        if site =='ag':
            regex_list = self.ag
        elif site =='rent':
            regex_list = self.rent
        else:
            regex_list = self.rentals
        for key, val in regex_list.items():
            if re.match(val, x):
                return key.split("@")[1]
