1) Get data pull from Redfin's house rentals. 
	TODO: AUTO REFRESH REDFIN DATA
2) Update base datasets (Programitcally and or manually)
cheapestCities_datawrapper.py
3) Create file system for outputs for cheapest_datawrapper.py.  (different for each projects.).  This is done with states/create_dirs.sh
3) Run cheapestCities_datawrapper.py 
	outputs: Data formatted for state/State_ab/(Table|Map|Timeseries) 
4) Subsequently, pipeline.py runs appended to cheapests 
	*ENSURE* configs.txt first_run = TRUE
		This will make all chart_ids which will be taken of for when we update in the future. 
	*TODO*: HAVE NEW FILE FOR DIFFERENT CONFIGS OF GRAPHS. 
	TODO: MASS DELETE CHARTID FUCNTION

When creating a new chart/project.  make sure to create it on datawrapper.com.  THen you will want to request via API the configs of that public published visual.  Copy the configs chart_configs.
	
5) Create ChatGPT output. (Done within ipynb) Outputs gpt output to each states/STATE/ folder
6) Format gpt outputs (Done with a ipynb).   
