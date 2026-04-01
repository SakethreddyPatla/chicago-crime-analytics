import pandas as pd
import requests
import os
SOCRATA_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
APP_TOKEN   = os.getenv("SOCRATA_APP_TOKEN") 
headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
params = {"$limit": 1000}
response = requests.get(SOCRATA_URL, headers=headers, params={"$limit": 5})
if response.status_code == 200:
    # Load JSON data into a DataFrame
    df = pd.DataFrame(response.json())
    
    # Filter to see only the Location and Computed Region columns
    computed_cols = [col for col in df.columns if "computed_region" in col.lower()]
    #print(df[['id', 'case_number'] + computed_cols].head())
    #print(df[computed_cols])
    print(df.columns)
else:
    print(f"Error: {response.status_code}, {response.text}")