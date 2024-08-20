import os

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


# Import the correct exception class
# from requests.exceptions import HTTPError

# url = "http://localhost:3000/albums/"
# try:
#     r = requests.get(url)
#     # Enable raising errors for all error status_codes
#     r.raise_for_status()
#     print(r.status_code)
# # Intercept the error
# except HTTPError as http_err:
#     print(f"HTTP error occurred: {http_err}")

api_key = os.getenv("CFDB_API_KEY")
base_url = "https://api.collegefootballdata.com/"
year = 2022
interest = "games"
headers = {"accept": "application/json", "Authorization": f"Bearer {api_key}"}
params = {"year": year, "seasonType": "regular"}


try:
    r = requests.get(
        base_url + interest,
        params=params,
        headers=headers,
        timeout=10,
    )
    r.raise_for_status()
    print(r.status_code)
except requests.exceptions.HTTPError as errh:
    print("Http Error:", errh)
except requests.exceptions.ConnectionError as errc:
    print("Error Connecting:", errc)
except requests.exceptions.Timeout as errt:
    print("Timeout Error:", errt)
except requests.exceptions.RequestException as err:
    print("Oops: Something Else", err)

data = r.json()
print(f"Response Length: {len(data)}")

df = pd.json_normalize(data)
df.to_csv(f"data/{interest}_{year}.csv", index=False)
