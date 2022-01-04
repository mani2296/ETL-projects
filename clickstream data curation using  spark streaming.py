from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
""" dataset:
{"results":[{"gender":"female","name":{"title":"Mrs","first":"Elizabeth","last":"Wilson"},"location":{"street":{"number":2593,"name":"Buckleys Road"},"city":"Whangarei","state":"Waikato","country":"New Zealand","postcode":28352,"coordinates":{"latitude":"60.5515","longitude":"163.4887"},"timezone":{"offset":"-7:00","description":"Mountain Time (US & Canada)"}},"email":"elizabeth.wilson@example.com","login":{"uuid":"3e8241fd-3349-4552-8192-054972f07b62","username":"blackbird384","password":"manman","salt":"nqJuukHg","md5":"dbd0f0695e2946a2308a14613f8becb7","sha1":"5f2c5ea7cd1015f0cf5078330c0be999637277dc","sha256":"adf822e1700c2da7d465c303288eb2f2d90b9636e1fc143fe53c3c3fbb669239"},"dob":{"date":"1959-12-27T16:28:20.716Z","age":62},"registered":{"date":"2012-03-23T18:32:53.035Z","age":9},"phone":"(190)-474-1961","cell":"(141)-796-1046","id":{"name":"","value":null},"picture":{"large":"https://randomuser.me/api/portraits/women/45.jpg","medium":"https://randomuser.me/api/portraits/med/women/45.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/women/45.jpg"},"nat":"NZ"}],"info":{"seed":"adb5275ce78158aa","results":1,"page":1,"version":"1.3"}}
{"results":[{"gender":"female","name":{"title":"Mrs","first":"Ines","last":"Gutierrez"},"location":{"street":{"number":872,"name":"Calle del Pez"},"city":"Vigo","state":"Ceuta","country":"Spain","postcode":62993,"coordinates":{"latitude":"-23.6431","longitude":"80.6090"},"timezone":{"offset":"-10:00","description":"Hawaii"}},"email":"ines.gutierrez@example.com","login":{"uuid":"66711cf6-92d4-4682-a851-a9c563e2c5c1","username":"bigostrich616","password":"flip","salt":"iYqyFkkF","md5":"289830c99721fd2a0355ae8d2c0ccf03","sha1":"f6f8b88c67ed886596bb54c36142b2c43aab0f01","sha256":"24da29cdbe7c6203a2ad7ca89a46135caef1d352c1cd8ec999b103d3d223ce79"},"dob":{"date":"1987-12-03T00:41:51.551Z","age":34},"registered":{"date":"2008-06-30T18:23:41.819Z","age":13},"phone":"929-173-700","cell":"664-746-540","id":{"name":"DNI","value":"08504605-A"},"picture":{"large":"https://randomuser.me/api/portraits/women/37.jpg","medium":"https://randomuser.me/api/portraits/med/women/37.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/women/37.jpg"},"nat":"ES"}],"info":{"seed":"e71d5453389c3530","results":1,"page":1,"version":"1.3"}}
{"results":[{"gender":"female","name":{"title":"Ms","first":"Elif","last":"Sepetçi"},"location":{"street":{"number":8311,"name":"Filistin Cd"},"city":"Çorum","state":"Şırnak","country":"Turkey","postcode":20468,"coordinates":{"latitude":"79.1539","longitude":"84.6990"},"timezone":{"offset":"-7:00","description":"Mountain Time (US & Canada)"}},"email":"elif.sepetci@example.com","login":{"uuid":"5ba2d93e-11e1-49c5-a657-c43a2d7258c7","username":"heavyrabbit332","password":"foot","salt":"g3hMXw5z","md5":"93f9e02fe2ddd8124eb4afc97b1aacec","sha1":"99ce9c153515ed443aca6f9d5d6e71533f30b1a4","sha256":"5832bc45ac6645ae6e84e44dfa01db8e86c6bf2b73232c55ed1d0488d175ea3a"},"dob":{"date":"1956-11-05T09:18:13.864Z","age":65},"registered":{"date":"2013-06-15T11:04:55.207Z","age":8},"phone":"(841)-357-4107","cell":"(712)-114-2830","id":{"name":"","value":null},"picture":{"large":"https://randomuser.me/api/portraits/women/12.jpg","medium":"https://randomuser.me/api/portraits/med/women/12.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/women/12.jpg"},"nat":"TR"}],"info":{"seed":"0ed716a226fad84c","results":1,"page":1,"version":"1.3"}}
"""
"""
Description:
In this json data from source is transfered using nifi to kafka. From kafka topic clickstream data is then read and cleaned using spark.
The records from kafka will be in binary format key value pairs so we used casting and applied from_json function to give proper structure and applied 
various transformations. Finally again transfered the curated data back to new kafka topic outputstream. Here we used trigger time as 20 seconds so
for each 20 seconds it will acculate data to form a micro batch process it and then write to kafka topic. Again to write to kafka topic data should in key value pair.
"""
if __name__=="__main__":
    spark=SparkSession.builder.appName("streaming hackathon") \
        .master("local[*]").config("spark.sql.shuffle.partitions",2)\
        .config("spark.streaming.stopGracefullyOnShutdown","true") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")\
        .config("spark.sql.streaming.schemaInference","true")\
        .getOrCreate()
    
    df=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","clickstream")\
     .option("startingOffsets","earliest").option("failonDataLoss","False").load()
     
    myschema=StructType([
    StructField("results",ArrayType(StructType([
    StructField("gender",StringType()),
    StructField("name",StructType([StructField("title",StringType()),StructField("first",StringType()),StructField("last",StringType())])),
    StructField("location",StructType([StructField("street",StructType([StructField("number",LongType()),StructField("name",StringType())])),StructField("city",StringType()),StructField("state",StringType()),StructField("country",StringType()),
    StructField("postcode",LongType()),StructField("coordinates",StructType([StructField("latitude",StringType()),StructField("longitude",StringType())])),StructField("timezone",StructType([StructField("offset",StringType()),StructField("description",StringType())]))])),
    StructField("email",StringType()),   
    StructField("login",StructType([StructField("uuid",StringType()),StructField("username",StringType()),StructField("password",StringType()),StructField("salt",StringType()),StructField("md5",StringType()),StructField("sha1",StringType()),StructField("sha256",StringType())])), 
    StructField("dob",StructType([StructField("date",TimestampType()),StructField("age",IntegerType())])),
    StructField("registered",StructType([StructField("date",TimestampType()),StructField("age",IntegerType())])), 
    StructField("phone",StringType()),
    StructField("cell",StringType()),
    StructField("id",StructType([StructField("name",StringType()),StructField("value",StringType())])),
    StructField("picture",StructType([StructField("large",StringType()),StructField("medium",StringType()),StructField("thumbnail",StringType())])),
    StructField("nat",StringType())]))),
    StructField("info",StructType([StructField("seed",StringType()),StructField("results",LongType()),StructField("page",LongType()),StructField("version",StringType())]))
]) 
    
    
    value_df=df.select(from_json(col("value").cast("string"),myschema).alias("value"))
    
    
    flatten_df=value_df.select("value.*")
    
    flatten_df.printSchema()
    
    df1=flatten_df.selectExpr("explode(results) as res","info.page as page","res.cell as cell","res.name.first as first","res.dob.age as age",
                 "res.dob.date as dobdt","res.email as email","res.location.city as uscity","res.location.coordinates.latitude as latitude",
                 "res.location.coordinates.longitude as longitude","res.location.country as country",
                 "res.location.state as state","res.location.timezone as timezone","res.login.username as username",
                  "current_date as curdt","current_timestamp as curts")

    
    df2=df1.filter("age>25")
    
    cleaned_df=df2.selectExpr("username as custid","cell","first","age","email","uscity","concat(latitude,',',longitude) as coordinates", 
               "regexp_replace(cell,'[()-]','') as cellnumber","country","state","date(dobdt) as dobdt",
               "int(round(datediff(current_date,dobdt)/365)) as age_flag","age",
               "case when int(round(datediff(current_date,dobdt)/365))==age then 'true' else 'false' end as age_match",
              "current_date as sysdt","current_timestamp as systs")
    
    
    kafka_final_df=cleaned_df.selectExpr("custid as key","to_json(struct(*)) as value")
    
    kafka_write=kafka_final_df.writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092") \
                .option("topic","outputstream").outputMode("update").trigger(processingTime='20 seconds') \
                .option("checkpointLocation","/home/hduser/checkpoint_kafka").option("queryName","clickstream_data").start()
    
    kafka_write.awaitTermination()
