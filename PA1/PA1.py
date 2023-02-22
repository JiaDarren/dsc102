from cProfile import run
from dask.distributed import Client, LocalCluster
import time
import json

import re
import pandas as pd
import dask.dataframe as dd
import dask

def PA1(user_reviews_csv,products_csv):
    start = time.time()
    client = Client('127.0.0.1:8786')
    client = client.restart()
    print(client)
        
    #######################
    # YOUR CODE GOES HERE #
    ####################### 
    
    products = dd.read_csv(products_csv)
    reviews = dd.read_csv(user_reviews_csv)

    # Q1
    q1_reviews = reviews.isna().mean().compute().round(4) * 100
    q1_products = products.isna().mean().compute().round(4) * 100


    # Set up for Q1-Q6
    products = dd.read_csv(products_csv, 
                            usecols=['price',  
                                    'categories', 
                                    'asin',
                                    'related'
                                    ])
    reviews = dd.read_csv(user_reviews_csv, 
                          usecols=['asin', 'overall'])

    bag1 = {}

    # Q2
    subproducts = products[['asin', 'price']]
    subreviews = reviews[['asin', 'overall']]

    joined = dd.merge(subproducts, subreviews, on = 'asin', how = 'inner')


    bag1['q2'] = joined.corr()['overall']

    # Q3
    price_prod = products['price'].describe()
    bag1['q3'] = price_prod

    # Q4
    def get_supercat(x):
        if isinstance(x, float):
            return x
        else:
            return eval(x)[0][0]

    numCat = products.assign(categories = lambda x: x.categories.apply(
        get_supercat,
        meta=('categories', 'str'))
                            )

    numCat = numCat.groupby("categories").count()['asin']
    bag1['q4'] = numCat


    # Q5
    subR = reviews[['asin']]
    subP = products[["asin"]]
    subP["temp"] = 1

    left = dd.merge(subR, subP, how = "left")
    bag1['q5'] = left.temp.isna().any()


    # Q6
    def g(ser):
        lst = []
        def parse(related):
            if (not pd.isnull(related)):
                lst.extend(re.findall("[A-Z0-9]{10}", related))

        ser.apply(parse)
        return pd.Series(lst)

    def f(df):
        return df.apply(g)

    subR = products[["related"]]
    metadata = {"related":"str"}
    res = subR.map_partitions(f, meta = metadata)
    res = res.rename(columns={"related": "asin"})
    res = res.drop_duplicates(split_out=16)

    subA = products[["asin"]]
    subA["tempA"] = 1
    res2 = res
    res2["tempR"] = 1
    jn = subA.merge(res2, how='outer', on='asin')

    q6 = (jn.tempA.isna().any() + jn.tempR.isna().any()).compute() > 0
    
    # Post-processing
    temp1 = dask.compute(bag1)
    temp1 = temp1[0]  

    q2 = float(temp1['q2']['price'].round(2))
    q3 = temp1['q3'][['mean', 'std', '50%', 'min', 'max']].round(2).astype(float)
    q4 = temp1['q4'][1:].sort_index(ascending=True, kind="mergesort").sort_values(ascending=False, kind="mergesort").astype(int)
    q5 = int(temp1['q5'] + 0)
    q6 = int(q6 + 0)
    
    
    end = time.time()
    runtime = end-start

    # Write your results to "results_PA1.json" here
    with open('OutputSchema_PA1.json','r') as json_file:
        data = json.load(json_file)
        print(data)
        
        data['q1']['reviews'] = json.loads(q1_reviews.to_json())
        data['q1']['products'] = json.loads(q1_products.to_json())
        data['q2'] = q2
        data['q3'] = json.loads(q3.to_json())
        data['q4'] = json.loads(q4.to_json())
        data['q5'] = q5
        data['q6'] = q6
    # print(data)
    with open('results_PA1.json', 'w') as outfile: json.dump(data, outfile)


    return runtime