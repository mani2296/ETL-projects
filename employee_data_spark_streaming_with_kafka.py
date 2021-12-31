from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
""" dataset:
{"agent_id":1,"name":"Cliff","sal":100000,"desig":["SWE","Analyst","SAnalyst","PL"],"address":{"dno":4,"street":"Castlepoint blvd","area":"Piscatway","city":"NJ","pin":10043},"other_info":{"Marital Status":"M","Education":"MS","Language":"Eng"}}
{"agent_id":2,"name":"Joe","sal":120000,"desig":["SWE","Analyst","SAnalyst","PM"],"address":{"dno":8,"street":"Camaron 3rd street","area":"Queens","city":"NY","pin":20088},"other_info":{"Marital Status":"M","Education":"BS","Language":"Eng"}}
{"agent_id":3,"name":"Aravind","sal":130000,"desig":["SWE","Analyst","SAnalyst","PM"],"address":{"dno":3,"street":"Doveton street","area":"Purasai","city":"CHN","pin":600022},"other_info":{"Marital Status":"U","Education":"BE","Dept":"IT"}}
{"agent_id":4,"name":"Gokul","sal":80000,"desig":["SWE","Analyst"],"address":{"dno":31,"street":"Mint Street","area":"Broadway","city":"CHN","pin":600001},"other_info":{"Marital Status":"U","Education":"BSc","Bond":"Yes"}}

"""
"""Description
In this employee details dataset we get the data from kafka topic. Data from kafka will be in key value pairs and also in binary format so we convert that to appropriate format 
using from_json function(for from_json function u need to pass schema as argument) after that we applied some basic cleaning of data and output the result again back to the 
kafka. Before sending data to kafka u need to convert the result set into key value pairs as kafka accepts only key value pairs.
""""
if __name__ == "__main__":
    spark=SparkSession.builder.master("local[*]")\
        .config("spark.streaming.stopGracefullyOnShutdown","true")\
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.sql.streaming.schemaInference","true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()
        
    kafka_df=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","invoices1") \
        .option("startingOffsets","earliest").load()
        
    
    
    myschema=StructType([StructField("agent_id",StringType()),StructField("name",StringType()),StructField("sal",IntegerType()),
    StructField("desig",ArrayType(StringType())),StructField("address",StructType([StructField("dno",IntegerType()),
    StructField("street",StringType()),StructField("area",StringType()),StructField("city",StringType()),StructField("pin",IntegerType())])),
    StructField("other_info",StructType([StructField("Marital Status",StringType()),StructField("Education",StringType()),StructField("Language",StringType())]))])
    
    kafka_df.printSchema()
    
    value_df=kafka_df.select(from_json(col("value").cast("String"),myschema).alias("value"))
    
    flatten_df=value_df.select("value.*")
    
    
    clean_df=flatten_df.selectExpr("agent_id","name","sal","explode(desig) as designation","address.city as city","address.pin as pin","other_info.Language as Language","other_info.Education as education",
                          "case when sal>=100000 then 'High grade' when sal>=50000 then 'Medium grade' else 'Low grade' end as grade_level")
    
    
    df_out = clean_df.select(expr("agent_id as key"),to_json(struct("*")).alias("value"))
    
    kafka_write=df_out.writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").outputMode("update").triger("processingTime=30 seconds") \
                .option("topic","invoiceout").option("checkpointLocation","/home/hduser/checkpoint_kafka").start()
            
    kafka_write.awaitTermination()
