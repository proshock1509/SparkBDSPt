from pyspark.sql import SparkSession
import pymongo 
import json
import pandas as pd 
spark = SparkSession.builder \
    .master("local") \
    .appName("Export Data to MongoDB") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
db = myclient["BDS"]

def flatten_json(y): #input la dictionary
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    
    flatten(y)
    return out #output la dictionary
    
def divide_data(f_name):
    file = open(f_name, "r")

    for line in file:
        
        line_js = json.loads(line) #convert string to dictionary
        line_js = flatten_json(line_js) # flatten
        
        if (line[1:7] == '"city"'):
            #print(line_js,file = f1)
            # db.flatten_data.insert_one(line_js)
            db.flatten_data.insert_one(line_js)
            
                
        # else:
                # db.not_use_data.insert_one(line_js)
    print("\nSuccessful to MongoDB")
                

