# include your import statements here
import dask.dataframe as dd
from dask.distributed import Client
import json
import re

def PA0(user_reviews_csv):
    client = Client()
    client = client.restart()
    
    
    ddf = dd.read_csv(user_reviews_csv, usecols=['reviewerID', 'overall','helpful','reviewTime'])


    ddf = ddf.assign(reviewYear = lambda x: x.reviewTime.apply(
        lambda x: int(re.search(r"\d{4}", x).group()),
        meta=('reviewTime', 'str')))

    def addHelpUnhelp(df):
        helpful_series = df['helpful'].apply(lambda x: [int(i) for i in re.findall(r"\d+", x)])
        df[['newHelpful', 'totalreviews']] = helpful_series.to_list()
        return df

    metadata = {'reviewerID': 'str',
                'helpful': 'str',
                'overall': 'float',
                'reviewTime': 'str',
                'reviewYear': 'float',
                'newHelpful' : 'float',
                'totalreviews': 'float'
                }
    ddf = ddf.map_partitions(addHelpUnhelp,meta=metadata)
    
    ret = ddf.groupby('reviewerID').aggregate({
        'reviewTime' : 'count',
        'overall' : 'mean',
        'reviewYear' : 'min',
        'newHelpful' : 'sum',
        'totalreviews' : 'sum',
    }, split_out = 8)

    ret = ret.reset_index().rename(columns={
                        "reviewTime":"number_products_rated", 
                        "overall":"avg_ratings", 
                        "reviewYear":"reviewing_since", 
                        "newHelpful":"helpful_votes", 
                        "totalreviews":"total_votes"})

    submit = ret.describe().compute().round(2)
    with open('results_PA0.json', 'w') as outfile: json.dump(json.loads(submit.to_json()), outfile)
        
# do not type anything outside this function, we would be calling your function from a different python notebook
# and passing in our hidden dataset as the parameter, which should then function correctly.