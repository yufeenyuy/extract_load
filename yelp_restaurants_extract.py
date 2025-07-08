import requests
import pandas as pd
import os 
import logger as log
import postgresops as ps

from dotenv import load_dotenv

load_dotenv()


header = {
    "Authorization": f"Bearer {os.getenv('YELP_API_K')}",
    "accept": "application/json",
    "language": "de_DE"
}

#search criteria that will be included in the endpoint url stated in the header.
parameter = {
    "term" : "restaurants",
    "price" : "1,2,3"
}

#location is a parameter that only accepts strings unlike price that accepts list.
location = ["Bielefeld","Hamm","Paderborn","Essen","Dortmund","Cologne","Dusseldorf","Bochum", "Hanover"]

def get_businesses(dfs= None, df_lis = []) -> tuple:
    for place in location:
        try:
            response = requests.get(url="https://api.yelp.com/v3/businesses/search", params={**parameter, "location":place},headers=header)
            if response.status_code == 200:
                df = pd.DataFrame(response.json()['businesses'])
                if not df.empty:
                    df_lis.append(df)
                else:
                    log.lg.info(f"No data for {place} in yelp endpoint is found.")
            else:
                log.lg.info(f"The response code from yelp endpoint for location {place} is {response.status_code}.")
        except Exception as e:
            log.lg.info(f"This exception:{e} is from the get_businesses yelp endpoint.")
    business_ids = None
    if df_lis:
        dfs = pd.concat(df_lis, ignore_index=True)
        business_ids = set(dfs['id'])
    return dfs, list(set(business_ids))



def get_business_reviews(reviews_df = None):
    review_dfs =[]
    businesses_df = get_businesses()[0]
    business_ids = get_businesses()[1]
    for id in business_ids:
        try:
            response = requests.get(url=f"https://api.yelp.com/v3/businesses/{id}/reviews", headers=header)
            if response.status_code == 200:
                df = pd.DataFrame(response.json()['reviews'])
                df['business_id'] = id
                if not df.empty:
                    review_dfs.append(df)
                else:
                    log.lg.info(f"No reviews for {id} in yelp endpoint is found.")
            else:
                log.lg.info(f"The response code from yelp endpoint for business_id: {id} is {response.status_code}.")
        except Exception as e:
            log.lg.info(f"This exception:{e} is from the get_businesses yelp endpoint.")
    if review_dfs:
        reviews_df = pd.concat(review_dfs, ignore_index=True)
    return businesses_df, reviews_df


