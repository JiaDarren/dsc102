# include your import statements here
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
import json


def PA0(user_reviews_csv):
    client = Client()
	client = client.restart()

    # ensure to use the parameter name i.e user_reviews_csv instead of hardcoding the filename inside read_csv func
    # for eg. dd.read_csv('user_reviews_Release.csv') is hardcoding and incorrect
    # instead leave it as dd.read_csv(user_reviews_csv) that is the parameter as set in the function signature.

    

    # ensure that you have replaced <YOUR_USERS_DATAFRAME> with your final dataframe.
    submit = <YOUR_USERS_DATAFRAME>.describe().compute().round(2)    
    with open('results_PA0.json', 'w') as outfile: json.dump(json.loads(submit.to_json()), outfile)

# do not type anything outside this function, we would be calling your function from a different python notebook
# and passing in our hidden dataset as the parameter, which should then function correctly.