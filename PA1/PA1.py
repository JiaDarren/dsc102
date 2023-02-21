from cProfile import run
from dask.distributed import Client, LocalCluster
import time
import json


def PA1(user_reviews_csv,products_csv):
    start = time.time()
    client = Client('127.0.0.1:8786')
    client = client.restart()
    print(client)
        
    #######################
    # YOUR CODE GOES HERE #
    ####################### 

    q1_reviews = None
    q1_products = None
    q2 = None
    q3 = None
    q4 = None
    q5 = None
    q6 = None
    end = time.time()
    runtime = end-start

    # Write your results to "results_PA1.json" here
    with open('OutputSchema_PA1.json','r') as json_file:
        data = json.load(json_file)
        print(data)

        data['q1']['products'] = json.loads(q1_reviews.to_json())
        data['q1']['reviews'] = json.loads(q1_products.to_json())
        data['q2'] = q2
        data['q3'] = json.loads(q3.to_json())
        data['q4'] = json.loads(q4.to_json())
        data['q5'] = q5
        data['q6'] = q6
    
    # print(data)
    with open('results_PA1.json', 'w') as outfile: json.dump(data, outfile)


    return runtime