from pyspark.sql import SparkSession
from pyspark.sql.functions import *
""" dataset
dateRep	day	month	year	cases	deaths	countriesAndTerritories	geoId	countryterritoryCode	popData2019	continentExp	Cumulative_number_for_14_days_of_COVID-19_cases_per_100000
14-12-2020	14	12	2020	746	6	Afghanistan	AF	AFG	38041757	Asia	9.01377925
13-12-2020	13	12	2020	298	9	Afghanistan	AF	AFG	38041757	Asia	7.05277624
12-12-2020	12	12	2020	113	11	Afghanistan	AF	AFG	38041757	Asia	6.86876792
"""


spark=SparkSession.builder.appName("covid-19 data").getOrCreate()

df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load(r"C:\tmp\COVID-19 data.csv")

df1.show(1,False)

df1.printSchema()

df_c=df1.selectExpr("to_date(dateRep,'dd-MM-yyyy') as dateRep1","*").drop("dateRep")
df_c.show()

month_agg=df_c.groupBy("countriesAndTerritories","year","month").agg(expr("sum(cases) as total_cases"),expr("sum(deaths) as total_deaths")).orderBy(col("total_deaths").desc())
month_agg.show()

month_agg.coalesce(1).write.format("csv").partitionBy("countriesAndTerritories").mode("overwrite").save(r"C:\Users\manig\Documents\output\month_agg")


country_agg=df_c.groupBy("countriesAndTerritories").agg(expr("sum(cases) as total_cases"),expr("sum(deaths) as total_deaths")).orderBy(col("total_deaths").desc())
country_agg.show()

country_agg.coalesce(1).write.format("csv").mode("overwrite").save(r"C:\Users\manig\Documents\output\country_agg")

continent_agg=df_c.groupBy("continentExp").agg(expr("sum(cases) as total_cases"),expr("sum(deaths) as total_deaths")).orderBy(col("total_deaths").desc())
continent_agg.show()

continent_agg.coalesce(1).write.format("csv").mode("overwrite").save(r"C:\Users\manig\Documents\output\continent_agg")
