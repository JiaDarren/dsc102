import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from utilities import SEED
# import any other dependencies you want, but make sure only to use the ones
# availiable on AWS EMR

# ---------------- choose input format, dataframe or rdd ----------------------
INPUT_FORMAT = 'dataframe'  # change to 'rdd' if you wish to use rdd inputs
# -----------------------------------------------------------------------------
if INPUT_FORMAT == 'dataframe':
    import pyspark.ml as M
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
if INPUT_FORMAT == 'koalas':
    import databricks.koalas as ks
elif INPUT_FORMAT == 'rdd':
    import pyspark.mllib as M
    from pyspark.mllib.feature import Word2Vec
    from pyspark.mllib.linalg import Vectors
    from pyspark.mllib.linalg.distributed import RowMatrix
    from pyspark.mllib.tree import DecisionTree
    from pyspark.mllib.regression import LabeledPoint
    from pyspark.mllib.linalg import DenseVector
    from pyspark.mllib.evaluation import RegressionMetrics

import math
# ---------- Begin definition of helper functions, if you need any ------------

# def task_1_helper():
#   pass

# -----------------------------------------------------------------------------


# %load -s task_1 assignment2.py
def task_1(data_io, review_data, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    overall_column = 'overall'
    # Outputs:
    mean_rating_column = 'meanRating'
    count_rating_column = 'countRating'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    groups = review_data.groupby("asin")
    
    newRev = groups.agg(
        F.avg("overall").alias("meanRating"),
        F.count("overall").alias("countRating")
    )

    
    newPrd = product_data.join(newRev, "asin", "left").select("asin", "meanRating", "countRating")
    
    vals = newPrd.select(
                        F.count("asin").alias("count_total"),
                        F.mean("meanRating").alias("mean_meanRating"), 
                        F.mean("countRating").alias("mean_countRating"),
                        F.variance("meanRating").alias("variance_meanRating"), 
                        F.variance("countRating").alias("variance_countRating"),
                        F.count(F.when(F.col("meanRating").isNull() , "meanRating")).alias("numNulls_meanRating"),
                        F.count(F.when(F.col("countRating").isNull() , "countRating")).alias("numNulls_countRating"),
                       ).collect()[0]
    
    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    # Calculate the values programmaticly. Do not change the keys and do not
    # hard-code values in the dict. Your submission will be evaluated with
    # different inputs.
    # Modify the values of the following dictionary accordingly.
    res = {
        'count_total': vals["count_total"],
        'mean_meanRating': vals["mean_meanRating"],
        'variance_meanRating': vals["variance_meanRating"],
        'numNulls_meanRating': vals["numNulls_meanRating"],
        'mean_countRating': vals["mean_countRating"],
        'variance_countRating': vals["variance_countRating"],
        'numNulls_countRating': vals["numNulls_countRating"]
    }


    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_1')
    return res
    # -------------------------------------------------------------------------


# %load -s task_2 assignment2.py
def task_2(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    salesRank_column = 'salesRank'
    categories_column = 'categories'
    asin_column = 'asin'
    # Outputs:
    category_column = 'category'
    bestSalesCategory_column = 'bestSalesCategory'
    bestSalesRank_column = 'bestSalesRank'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    
    flat_data = product_data.withColumn(
            "category", 
            F.when(
                F.col("categories").getItem(0).getItem(0) != '', 
                F.col("categories").getItem(0).getItem(0)
            ).otherwise(None)
        ).withColumn(
            "bestSalesCategory", 
            F.map_keys(product_data["salesRank"]).getItem(0)
        ).withColumn(
            "bestSalesRank", 
            F.map_values(product_data["salesRank"]).getItem(0)
        ).select('asin', 'category', 'salesRank', 'bestSalesCategory', 'bestSalesRank')
    

    vals = flat_data.select(
        F.count("asin").alias("count_total"),
        F.mean("bestSalesRank").alias("mean_bestSalesRank"), 
        F.variance("bestSalesRank").alias("variance_bestSalesRank"),
        F.count(F.when(F.col("category").isNull() , "category")).alias("numNulls_category"),
        F.count(F.when(F.col("bestSalesRank").isNull() , "bestSalesRank")).alias("numNulls_bestSalesCategory"),
        F.countDistinct("category").alias("countDistinct_category"),
        F.countDistinct("bestSalesCategory").alias("countDistinct_bestSalesCategory"),
    ).collect()[0]

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': vals['count_total'],
        'mean_bestSalesRank': vals['mean_bestSalesRank'],
        'variance_bestSalesRank': vals['variance_bestSalesRank'],
        'numNulls_category': vals['numNulls_category'],
        'countDistinct_category': vals['countDistinct_category'],
        'numNulls_bestSalesCategory': vals['numNulls_bestSalesCategory'],
        'countDistinct_bestSalesCategory': vals['countDistinct_bestSalesCategory']
    }


    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_2')
    return res
    # -------------------------------------------------------------------------


# %load -s task_3 assignment2.py
def task_3(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    price_column = 'price'
    attribute = 'also_viewed'
    related_column = 'related'
    # Outputs:
    meanPriceAlsoViewed_column = 'meanPriceAlsoViewed'
    countAlsoViewed_column = 'countAlsoViewed'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    
    also_viewed = product_data.select(
        product_data['asin'],
        product_data["related"].getItem('also_viewed').alias('also_viewed'),
        F.when(
            F.size(product_data["related"].getItem('also_viewed')) > 0,
            F.size(product_data["related"].getItem('also_viewed'))
        ).otherwise(None).alias('countAlsoViewed')
    )

    exploded_vals = also_viewed.withColumn(
        "exp_vals",
        F.explode(F.col('also_viewed'))
    ).select('asin', 'exp_vals')
#     exploded_vals.show(10)
    
    prices = product_data.select(F.col('asin').alias('pasin'), 'price').filter(F.col('price').isNotNull())
#     prices.show(10)

    exp_prices = prices.join(exploded_vals,prices.pasin == exploded_vals.exp_vals,'left')
#     exp_prices.show(10)
    
    merged_prices = exp_prices.groupby('asin').agg(
        F.mean('price').alias('meanPriceAlsoViewed')
    )
#     merged_prices.show(10)
    outcome = also_viewed.join(merged_prices, on = 'asin', how = 'left')
#     outcome.show(10)
    
    vals = outcome.select(
        F.count("asin").alias("count_total"),
        F.mean("meanPriceAlsoViewed").alias("mean_meanPriceAlsoViewed"),
        F.mean("countAlsoViewed").alias("mean_countAlsoViewed"),
        F.variance("meanPriceAlsoViewed").alias("variance_meanPriceAlsoViewed"),
        F.variance("countAlsoViewed").alias("variance_countAlsoViewed"),
        F.count(F.when(F.col("meanPriceAlsoViewed").isNull() , "meanPriceAlsoViewed")).alias("numNulls_meanPriceAlsoViewed"),
        F.count(F.when(F.col("countAlsoViewed").isNull() , "countAlsoViewed")).alias("numNulls_countAlsoViewed"),
    ).collect()[0]


    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': vals['count_total'],
        'mean_meanPriceAlsoViewed': vals['mean_meanPriceAlsoViewed'],
        'variance_meanPriceAlsoViewed': vals['variance_meanPriceAlsoViewed'],
        'numNulls_meanPriceAlsoViewed': vals['numNulls_meanPriceAlsoViewed'],
        'mean_countAlsoViewed': vals['mean_countAlsoViewed'],
        'variance_countAlsoViewed': vals['variance_countAlsoViewed'],
        'numNulls_countAlsoViewed': vals['numNulls_countAlsoViewed']
    }



    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_3')
    return res
    # -------------------------------------------------------------------------


# %load -s task_4 assignment2.py
def task_4(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    price_column = 'price'
    title_column = 'title'
    # Outputs:
    meanImputedPrice_column = 'meanImputedPrice'
    medianImputedPrice_column = 'medianImputedPrice'
    unknownImputedTitle_column = 'unknownImputedTitle'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    price = product_data.withColumn(
        'price', 
        F.col('price').cast('float')
    ).select('asin', 'price', 'title')

    price_agg = price.agg(
        F.mean('price').alias('mean'), 
        F.expr('percentile_approx(price, 0.5)').alias('median')
    ).collect()[0]
    
    price_data = price.select(
        'asin',
        F.when(
            F.col('price').isNull(), 
            price_agg['mean']
        ).otherwise(F.col('price')).alias('meanInputedPrice'),
        F.when(
            F.col('price').isNull(), 
            price_agg['median']
        ).otherwise(F.col('price')).alias('medianInputedPrice'),
        F.when(
            (F.col('title') == '' ) | F.col('title').isNull(), 
            'unknown'
        ).otherwise(F.col('title')).alias('unknownInputedTitle')
    )
    

    
    vals = price_data.select(
        F.count("asin").alias("count_total"),
        F.mean("meanInputedPrice").alias("mean_meanImputedPrice"),
        F.mean("medianInputedPrice").alias("mean_medianImputedPrice"),
        F.variance("meanInputedPrice").alias("variance_meanImputedPrice"),
        F.variance("medianInputedPrice").alias("variance_medianImputedPrice"),
        F.count(F.when(F.col("meanInputedPrice").isNull() , "meanInputedPrice")).alias("numNulls_meanImputedPrice"),
        F.count(F.when(F.col("medianInputedPrice").isNull() , "medianInputedPrice")).alias("numNulls_medianImputedPrice"),
        F.count(F.when(F.col("unknownInputedTitle") == 'unknown' , "unknownInputedTitle")).alias("numUnknowns_unknownImputedTitle")
    ).collect()[0]

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': vals['count_total'],
        'mean_meanImputedPrice': vals['mean_meanImputedPrice'],
        'variance_meanImputedPrice': vals['variance_meanImputedPrice'],
        'numNulls_meanImputedPrice': vals['numNulls_meanImputedPrice'],
        'mean_medianImputedPrice': vals['mean_medianImputedPrice'],
        'variance_medianImputedPrice': vals['variance_medianImputedPrice'],
        'numNulls_medianImputedPrice': vals['numNulls_medianImputedPrice'],
        'numUnknowns_unknownImputedTitle': vals['numUnknowns_unknownImputedTitle']
    }


    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_4')
    return res
    # -------------------------------------------------------------------------


# %load -s task_5 assignment2.py
def task_5(data_io, product_processed_data, word_0, word_1, word_2):
    # -----------------------------Column names--------------------------------
    # Inputs:
    title_column = 'title'
    # Outputs:
    titleArray_column = 'titleArray'
    titleVector_column = 'titleVector'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------

    splf = F.udf(lambda row: row.lower())

    product_processed_data_output = product_processed_data.withColumn(titleArray_column, 
                                        F.split(splf(title_column), " "))

    
    word2Vec = M.feature.Word2Vec(minCount=100, vectorSize=16, seed=SEED, numPartitions=4, inputCol=titleArray_column, outputCol=titleVector_column)
    
    model = word2Vec.fit(product_processed_data_output.select(F.col(titleArray_column)))

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'size_vocabulary': None,
        'word_0_synonyms': [(None, None), ],
        'word_1_synonyms': [(None, None), ],
        'word_2_synonyms': [(None, None), ]
    }
    # Modify res:
    res['count_total'] = product_processed_data_output.count()
    res['size_vocabulary'] = model.getVectors().count()
    for name, word in zip(
        ['word_0_synonyms', 'word_1_synonyms', 'word_2_synonyms'],
        [word_0, word_1, word_2]
    ):
        res[name] = model.findSynonymsArray(word, 10)
    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_5')
    return res
    # -------------------------------------------------------------------------


# %load -s task_6 assignment2.py
def task_6(data_io, product_processed_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    category_column = 'category'
    # Outputs:
    categoryIndex_column = 'categoryIndex'
    categoryOneHot_column = 'categoryOneHot'
    categoryPCA_column = 'categoryPCA'
    # -------------------------------------------------------------------------    

    # ---------------------- Your implementation begins------------------------
    # Apply StringIndexer tp category column
    cat_indexer = M.feature.StringIndexer(
        inputCol = 'category',
        outputCol = 'category_index'
    )
    prod_processed = cat_indexer.fit(product_processed_data).transform(product_processed_data)
    
    # Apply OneHotEncoder to indexed column 
    ohe = M.feature.OneHotEncoder(
        inputCol = 'category_index',
        outputCol = 'categoryOneHot',
        dropLast = False
    )
    prod_onehot = ohe.fit(prod_processed).transform(prod_processed)

    # Apply PCA to ohe column
    pca = M.feature.PCA(
        k = 15,
        inputCol = 'categoryOneHot',
        outputCol = 'categoryPCA'
    )
    output = pca.fit(prod_onehot).transform(prod_onehot)
#     output.show()
    
    # get vals
    summarizer = M.stat.Summarizer.metrics("mean")
    vals = output.select(
        F.count("asin").alias("count_total"),
        summarizer.summary(F.col('categoryOneHot')).alias('meanVector_categoryOneHot'),
        summarizer.summary(F.col('categoryPCA')).alias('meanVector_categoryPCA')
    ).collect()[0]

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': vals['count_total'],
        'meanVector_categoryOneHot': vals['meanVector_categoryOneHot'][0],
        'meanVector_categoryPCA': vals['meanVector_categoryPCA'][0]
    }
    # Modify res:
    print(res)



    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_6')
    return res
    # -------------------------------------------------------------------------
    
    
def task_7(data_io, train_data, test_data):
    
    # ---------------------- Your implementation begins------------------------
    dt = M.regression.DecisionTreeRegressor(maxDepth=5, featuresCol="features", labelCol="overall")
    
    model = dt.fit(train_data)
    
    result = model.transform(test_data)
    
    result = result.withColumn('SE', F.pow(F.col("overall") - F.col("prediction"), 2))
    
    MSE = result.select(F.mean(F.col("SE"))).collect()[0][0]
    
    RMSE = math.sqrt(MSE)
    
    # -------------------------------------------------------------------------
    
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': RMSE
    }
    # Modify res:


    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_7')
    return res
    # -------------------------------------------------------------------------
    
    
def task_8(data_io, train_data, test_data):
    
    # ---------------------- Your implementation begins------------------------
    train, valid = train_data.randomSplit([.75, .25], SEED)
    
    def trainDTR(depth, train):
        dt = M.regression.DecisionTreeRegressor(maxDepth=depth, featuresCol="features", labelCol="overall")
    
        model = dt.fit(train_data)
        
        return model
        
    def rmseDTR(model, data):
    
        result = model.transform(data)
    
        result = result.withColumn('SE', F.pow(F.col("overall") - F.col("prediction"), 2))
    
        MSE = result.select(F.mean(F.col("SE"))).collect()[0][0]
    
        RMSE = math.sqrt(MSE)
        
        return RMSE
    
    m5 = trainDTR(5, train)
    rmse5 = rmseDTR(m5, valid)
    
    m7 = trainDTR(7, train)
    rmse7 = rmseDTR(m7, valid)
    
    m9 = trainDTR(9, train)
    rmse9 = rmseDTR(m9, valid)
    
    m12 = trainDTR(12, train)
    rmse12 = rmseDTR(m12, valid)
    
    
    rmse = rmseDTR(m5, test_data)
    
    
    
    # -------------------------------------------------------------------------
    
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': rmse,
        'valid_rmse_depth_5': rmse5,
        'valid_rmse_depth_7': rmse7,
        'valid_rmse_depth_9': rmse9,
        'valid_rmse_depth_12': rmse12,
    }
    # Modify res:


    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_8')
    return res
    # -------------------------------------------------------------------------

