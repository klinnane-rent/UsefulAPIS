#!/bin/bash

# Array of US state abbreviations
states=("AL" "AK" "AZ" "AR" "CA" "CO" "CT" "DE" "FL" "GA" "HI" "ID" "IL" "IN" "IA" "KS" "KY" "LA" "ME" "MD" "MA" "MI" "MN" "MS" "MO" "MT" "NE" "NV" "NH" "NJ" "NM" "NY" "NC" "ND" "OH" "OK" "OR" "PA" "RI" "SC" "SD" "TN" "TX" "UT" "VT" "VA" "WA" "WV" "WI" "WY")

# Create directories for each state abbreviation
for state in "${states[@]}"
do
    mkdir -p "$state"/{Timeseries,Table,Map}
done

echo "Directories created successfully."
