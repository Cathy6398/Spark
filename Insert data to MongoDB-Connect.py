import json
import sys
from pymongo import MongoClient
import pandas as pd
import json
import csv
from pyspark.sql import SparkSession

client = MongoClient('mongodb+srv://CathuCathy6398:CathuCathy6398@cluster0.fskanzu.mongodb.net/?retryWrites=true&w=majority')
db = client.Covid
collection = db.Covid

df = pd.read_csv('Case.csv')
documents = df.to_dict('records')
collection.insert_many(documents)
