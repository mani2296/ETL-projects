from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
"""This dataset is provided by yelp which publish crowd-sourced reviews about businesses. u can get this dataset from kaggle 
{"business_id":"6iYb2HFDywm3zjuRg0shjw","name":"Oskar Blues Taproom","address":"921 Pearl St","city":"Boulder","state":"CO","postal_code":"80302","latitude":40.0175444,"longitude":-105.2833481,"stars":4.0,"review_count":86,"is_open":1,"attributes":{"RestaurantsTableService":"True","WiFi":"u'free'","BikeParking":"True","BusinessParking":"{'garage': False, 'street': True, 'validated': False, 'lot': False, 'valet': False}","BusinessAcceptsCreditCards":"True","RestaurantsReservations":"False","WheelchairAccessible":"True","Caters":"True","OutdoorSeating":"True","RestaurantsGoodForGroups":"True","HappyHour":"True","BusinessAcceptsBitcoin":"False","RestaurantsPriceRange2":"2","Ambience":"{'touristy': False, 'hipster': False, 'romantic': False, 'divey': False, 'intimate': False, 'trendy': False, 'upscale': False, 'classy': False, 'casual': True}","HasTV":"True","Alcohol":"'beer_and_wine'","GoodForMeal":"{'dessert': False, 'latenight': False, 'lunch': False, 'dinner': False, 'brunch': False, 'breakfast': False}","DogsAllowed":"False","RestaurantsTakeOut":"True","NoiseLevel":"u'average'","RestaurantsAttire":"'casual'","RestaurantsDelivery":"None"},"categories":"Gastropubs, Food, Beer Gardens, Restaurants, Bars, American (Traditional), Beer Bar, Nightlife, Breweries","hours":{"Monday":"11:0-23:0","Tuesday":"11:0-23:0","Wednesday":"11:0-23:0","Thursday":"11:0-23:0","Friday":"11:0-23:0","Saturday":"11:0-23:0","Sunday":"11:0-23:0"}}
{"business_id":"tCbdrRPZA0oiIYSmHG3J0w","name":"Flying Elephants at PDX","address":"7000 NE Airport Way","city":"Portland","state":"OR","postal_code":"97218","latitude":45.5889058992,"longitude":-122.5933307507,"stars":4.0,"review_count":126,"is_open":1,"attributes":{"RestaurantsTakeOut":"True","RestaurantsAttire":"u'casual'","GoodForKids":"True","BikeParking":"False","OutdoorSeating":"False","Ambience":"{'romantic': False, 'intimate': False, 'touristy': False, 'hipster': False, 'divey': False, 'classy': False, 'trendy': False, 'upscale': False, 'casual': True}","Caters":"True","RestaurantsReservations":"False","RestaurantsDelivery":"False","HasTV":"False","RestaurantsGoodForGroups":"False","BusinessAcceptsCreditCards":"True","NoiseLevel":"u'average'","ByAppointmentOnly":"False","RestaurantsPriceRange2":"2","WiFi":"u'free'","BusinessParking":"{'garage': True, 'street': False, 'validated': False, 'lot': False, 'valet': False}","Alcohol":"u'beer_and_wine'","GoodForMeal":"{'dessert': False, 'latenight': False, 'lunch': True, 'dinner': False, 'brunch': False, 'breakfast': True}"},"categories":"Salad, Soup, Sandwiches, Delis, Restaurants, Cafes, Vegetarian","hours":{"Monday":"5:0-18:0","Tuesday":"5:0-17:0","Wednesday":"5:0-18:0","Thursday":"5:0-18:0","Friday":"5:0-18:0","Saturday":"5:0-18:0","Sunday":"5:0-18:0"}}

In this dataset is cleaned and filered based on the required fields and found out highest rated restaurent in the location based on certain conditions 
"""

spark=SparkSession.builder.master("local[*]").appName("yelp dataset").getOrCreate()

df=spark.read.format("json").load(r"C:\Users\manig\Documents\Datasets\yelp_academic_dataset_business.json\yelp_academic_dataset_business.json")

df.printSchema()
from pyspark.sql.functions import expr, col

cleaned_df=df.select("name","attributes.Alcohol","attributes.BusinessAcceptsCreditCards","attributes.GoodForDancing",
         "attributes.BikeParking","attributes.OutdoorSeating","attributes.ByAppointmentOnly",
         "attributes.RestaurantsDelivery",
        expr("named_struct('address',address,'latitude',latitude,'longitude',longitude,'state',state,'city',city,'postal_code',postal_code) as address"),
         "business_id",expr("split(categories,',') as categories"),"is_open","hours.*","review_count","stars")
cleaned_df.show(20,False)

#udf to find restaurant opening time and closing time
def open_close(v:list):
    if v==None:
        return None
    else:
        l=v.split("-")
        return l

#registering as udf
timings=udf(open_close,ArrayType(StringType()))

#querying to find the opening and closing time for each day
df_m=cleaned_df.select("*",timings("Monday").alias("time_col")).selectExpr("name as Restaurant","categories","time_col[0] as mon_open","time_col[0] as mon_close")
df_m.show(10,False)
df_m.write.format("parquet").saveAsTable("Monday_timetable")

df_tue=cleaned_df.select("*",timings("Tuesday").alias("time_col")).selectExpr("name as Restaurant","categories","time_col[0] as Tuesday_open","time_col[0] as Tuesday_close")
df_tue.show(10,False)
df_tue.write.format("parquet").saveAsTable("Tuesday_timetable")

df_wed=cleaned_df.select("*",timings("Wednesday").alias("time_col")).selectExpr("name as Restaurant","categories","time_col[0] as Tuesday_open","time_col[0] as Tuesday_close")
df_wed.show(10,False)
df_wed.write.format("parquet").saveAsTable("Wednesday_timetable")

df_thu=cleaned_df.select("*",timings("Thursday").alias("time_col")).selectExpr("name as Restaurant","categories","time_col[0] as Tuesday_open","time_col[0] as Tuesday_close")
df_thu.show(10,False)
df_thu.write.format("parquet").saveAsTable("Thursday_timetable")

df_fri=cleaned_df.select("*",timings("Friday").alias("time_col")).selectExpr("name as Restaurant","categories","time_col[0] as Tuesday_open","time_col[0] as Tuesday_close")
df_fri.show(10,False)
df_fri.write.format("parquet").saveAsTable("Friday_timetable")

df_sat=cleaned_df.select("*",timings("Saturday").alias("time_col")).selectExpr("name as Restaurant","categories","time_col[0] as Tuesday_open","time_col[0] as Tuesday_close")
df_sat.show(10,False)
df_sat.write.format("parquet").saveAsTable("Saturday_timetable")

df_sun=cleaned_df.select("*",timings("Sunday").alias("time_col")).selectExpr("name as Restaurant","categories","time_col[0] as Tuesday_open","time_col[0] as Tuesday_close")
df_sun.show(10,False)
df_sun.write.format("parquet").saveAsTable("Sunday_timetable")


#applying multiple filters
rating=col("stars")>=4.0
review_count=col("review_count")>500
open_or_close=col("is_open")==1
category=col("categories").contains("Bars")
mul_filter=df.where(rating & review_count & open_or_close | category ) \
.select("name","address","stars","review_count","is_open","categories").orderBy(col("stars").desc(),col("review_count").desc())
mul_filter.show(20,False)
mul_filter.write.format("parquet").save("/output")

#finding average,lowest,highest rating for a restaurant
ratings_agg=df.groupBy("city","name","state")\
    .agg(expr("min(stars) as lowest_rating"),expr("max(stars) as max_rating"),expr("avg(stars) as avg_rating"),expr("sum(review_count) as review_count"))\
    .orderBy(col("avg_rating").desc(),col("review_count").desc())
ratings_agg.show()

ratings_agg.write.format("parquet").save("/output1")
