import findspark
findspark.init()
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col


from utils import readMySQL, importToMySQL, readCassandra_time

def process_customtrack_bid(df, custom_track):
    data= df.filter(col("custom_track")== custom_track)
    data= data.selectExpr("date(ts) as date", "hour(ts) as hour", "job_id", \
                            "publisher_id", "campaign_id", "group_id","bid")
    data= data.na.fill(0)
    data= data.groupBy("date", "hour", "job_id", "publisher_id", "campaign_id", "group_id") \
        .agg(
            round(F.avg("bid"), 2).alias("bid_set"),
            F.sum("bid").alias("spend_hour"),
            F.count("*").alias(custom_track)
        )
    return data

def process_customtrack_withoutBid(df, custom_track):
    data= df.filter(col("custom_track")== custom_track)
    data= data.selectExpr("date(ts) as date", "hour(ts) as hour", "job_id", \
                            "publisher_id", "campaign_id", "group_id","bid")
    data= data.na.fill(0)
    data= data.groupBy("date", "hour", "job_id", "publisher_id", "campaign_id", "group_id") \
        .agg(
            F.count("*").alias(custom_track)
        )
    return data

def join_df_final(list_df):    
    join_columns = ['job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id']
    total_df= list_df[0]
    for df in list_df[1:]:
        total_df= total_df.join(df, join_columns, "full")
    return total_df

def process_cassandra_data(df):
    customtrack= [process_customtrack_bid(df, customtrack) for customtrack in ["click"]]
    customtrack_withoutbid= [process_customtrack_withoutBid(df, customtrack) for customtrack in ["conversion", "qualified", "unqualified"]]
    customtrack.extend(customtrack_withoutbid)
    return join_df_final(customtrack)

def retrieve_company_data(spark):
    df= readMySQL(spark, "data_warehouse", "job")
    company= df.selectExpr("id as job_id", "company_id", "group_id", "campaign_id")
    return company

def main_task(spark, lastest_update_time):
    cassandra_df= readCassandra_time(spark, "tracking", "study_de", lastest_update_time)
    company_df= retrieve_company_data(spark)
    df= process_cassandra_data(cassandra_df)
    final_output = df.join(company_df,'job_id','left').drop(company_df.group_id).drop(company_df.campaign_id)
    final_output = final_output.select('job_id','date','hour','publisher_id','company_id','campaign_id','group_id','unqualified','qualified','conversion','click','bid_set','spend_hour')
    final_output = final_output.withColumnRenamed('date','dates') \
                               .withColumnRenamed('hour','hours') \
                               .withColumnRenamed('qualified','qualified_application') \
                               .withColumnRenamed('unqualified','disqualified_application').withColumnRenamed('click','clicks')
    final_output = final_output.withColumn('sources',F.lit('Cassandra'))
    final_output = final_output.withColumn('lastest_update_time', F.lit(cassandra_df.selectExpr("max(ts)").take(1)[0][0]))
    importToMySQL(final_output, "data_warehouse", "events")




if __name__ == "__main__":
    pass
