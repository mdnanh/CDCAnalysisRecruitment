from DE.Project1.Job_CDC.CassandraJobETL import main_task
from utils import readCassandra_time ,readMySQL, getSparkSession
import datetime
import time

def get_latest_time_cassandra(spark):
    cassandra_latest_time = spark.read \
                    .format("org.apache.spark.sql.cassandra") \
                    .options(table="tracking", keyspace="study_de") \
                    .load() \
                    .agg({"ts": "max"}).take(1)[0][0].replace(microsecond=0)

    return cassandra_latest_time


def get_latest_time_MySQL(spark):
    table= """(SELECT max(lastest_update_time) FROM events) lastest_update_time"""
    mysql_latest_time= readMySQL(spark, database="data_warehouse", nameTable=table)
    mysql_latest_time= mysql_latest_time.take(1)[0][0]
    if mysql_latest_time is None:
        mysql_latest_time = datetime.datetime(1998, 1, 1, 23, 59, 59)
    else : 
        mysql_latest_time = mysql_latest_time
    return mysql_latest_time



if __name__ == "__main__":

    config_spark= {"spark.jars.packages" : "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"}

    spark= getSparkSession(app_name= "ETLJob", config_options= config_spark)

    while True:
        start_time= time.time.now()
        mysql_time = get_latest_time_MySQL(spark)
        print("MySQL latest time is {}".format(mysql_time))
        data_new= readCassandra_time(spark, "tracking", "study_de", mysql_time)
        if data_new.count() > 0:
            cassandra_time= get_latest_time_cassandra(spark)
            print("Cassandra latest time is {}".format(cassandra_time))
            print("-------------------------")
            print("--------Main task--------")
            print("-------------------------")
            main_task(spark, mysql_time)
            print("------------------------")
            print("--------Finished--------")
            print("------------------------")
        else :
            cassandra_time= mysql_time
            print("Cassandra latest time is {}".format(cassandra_time))
            print("---------------------------------")
            print("--------No new data found--------")
            print("---------------------------------")
        end_time = datetime.datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        print('Job takes {} seconds to execute'.format(execution_time))
        time.sleep(5)
