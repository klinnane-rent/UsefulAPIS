REDFIN_METRIC_MAPPING = {
    # Sales API Keys:
    "# of Active Listings in Last 30 Days": "active_listing_count_30",
    "# of New Listings": "new_listing_count_30",

    "Median List Price": "list_price_median_30",
    "Median Sale Price": "sale_price_median_30",

    "Median List Price $/Sq Ft": "list_price_per_sqft_median_30",
    "Median Sale Price $/Sq Ft": "sale_price_per_sqft_median_30",

    "% of Homes Sold Within 2 Weeks": "pct_homes_sold_within_2weeks_month",
    "% Average Down Payment": "rf_avg_down_payment_pct_30",
    "Median Days on Market": "sold_dom_median_365",

    "# of Homes Sold in Last 30 Days": "sold_count_30",
    "# of Homes Sold in Last 365 Days": "sold_count_365",

    "% of Homes Sold More Than 5% Under List Price": "pct_homes_sold_5_percent_under_list_month",
    "% of Homes Sold Up to 5% Under List Price": "pct_homes_sold_0to5_percent_under_list_month",

    "% of Homes Sold at List Price": "pct_homes_sold_at_list_month",

    "% of Homes Sold Above List Price": "pct_homes_sold_above_list_month",
    "% of Homes Sold Up to 5% Above List Price": "pct_homes_sold_0to5_percent_above_list_month",
    "% of Homes Sold 5-10% Above List Price": "pct_homes_sold_5to10_percent_above_list_month",
    "% of Homes Sold More Than 10% Above List Price": "pct_homes_sold_10_percent_above_list_month",
    # Rental API Keys: / comes in same json.  Parsing done in redfin_api_client.py
    "Median Rent By Month And Property Type": 'medianRentByMonth',
    'MoM Median Rent Change By Property Type' : 'mom'
}
