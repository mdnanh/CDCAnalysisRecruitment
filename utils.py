from dotenv import load_dotenv
import os
load_dotenv() 
USERNAME = os.getenv('MYSQL_USER')
PASSWORK = os.getenv('MYSQL_PASSWORD')
MYSQL_URL= os.getenv('MYSQL_URL') 
MONGODB_URI = os.getenv('MONGODB_URI')

import warnings
warnings.filterwarnings("ignore")

import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def readData(spark, filename):
    return spark.read.parquet(filename)

def getSparkSession(app_name="MyApp", master="local[*]", config_options=None):
    """
    Create a Spark session with the given app name and optional configurations.
    
    Parameters:
    - app_name (str): Name of the Spark application.
    - master (str): The master URL for the cluster.
    - config_options (dict): Additional Spark configurations as key-value pairs.
    
    Returns:
    - SparkSession: A SparkSession object.
    """
    builder = SparkSession.builder.appName(app_name).master(master)
    
    # Apply additional configurations if provided
    if config_options:
        for key, value in config_options.items():
            builder = builder.config(key, value)
    
    return builder.getOrCreate()

def readMySQL(spark, database, nameTable):
    df= spark.read.format("jdbc").options(
        url= f"{MYSQL_URL + database}",
        driver = "com.mysql.cj.jdbc.Driver",
        dbtable = f"{nameTable}",
        user= USERNAME,
        password= PASSWORK).load()
    return df


def readMongoDB(spark, database, nameTable):
    df = spark.read \
        .format("mongodb") \
        .option("spark.mongodb.read.connection.uri", MONGODB_URI) \
        .option("spark.mongodb.write.connection.uri", MONGODB_URI) \
        .option("database", database) \
        .option("collection", nameTable) \
        .load()
    return df


def readCassandra_time(spark, table, keyspace, lastest_time):
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table= table,keyspace= keyspace) \
        .load() \
        .where(F.date_trunc("second", F.col("ts")) > lastest_time)
    return df

def readCassandra(spark, table, keyspace):
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table= table,keyspace= keyspace) \
        .load()
    return df


def importMongoDB(df, database, nameTable):
    print('----------------------------------------')
    print("---------Saving data to Database--------")
    print('----------------------------------------')
    df.write.format("mongodb") \
               .mode("append") \
               .option("database", database) \
               .option("collection", nameTable) \
               .save()
    print("--------Data Import Successfully--------")
    print("----------------------------------------")


def importToMySQL(df, database, nameTable):
    print('----------------------------------------')
    print(f"--------Saving data to Database--------")
    print('----------------------------------------')
    df.write.format("jdbc").options(
        url= f"{MYSQL_URL + database}",
        driver = "com.mysql.cj.jdbc.Driver",
        dbtable = f"{nameTable}",
        user= USERNAME,
        password= PASSWORK).mode('append').save()
    print("--------Data Import Successfully--------")
    print("----------------------------------------")

if __name__ == "__main__":
    pass