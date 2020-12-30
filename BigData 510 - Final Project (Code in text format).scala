// Databricks notebook source
// DBTITLE 1,Display path to yellow taxi data from container
display(dbutils.fs.ls("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData"))

// COMMAND ----------

// DBTITLE 1,Display path to Taxi lookup table data from container
display(dbutils.fs.ls("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/Taxi_LookupTable"))

// COMMAND ----------

// DBTITLE 1,Display path to weather data from container
display(dbutils.fs.ls("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/nyc_weatherdata"))

// COMMAND ----------

// DBTITLE 1,Loading NYC Yellow Taxicab data
//Loading NYC Yellow Taxicab data
val yellowtaxicab_data = spark.read.option("inferSchema", "true").option("header",true).csv("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-12.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-11.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-10.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-09.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-08.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-07.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-06.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-05.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-04.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-03.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-02.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2019-01.csv"
                                                             
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-01.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-02.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-03.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-04.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-05.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-06.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-07.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-08.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-09.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-10.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-11.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2018-12.csv"
                                                             
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-12.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-11.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-10.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-09.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-08.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-07.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-06.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-05.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-04.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-03.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-02.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2017-01.csv"
                                                                                            
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-12.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-11.csv"                                                          ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-10.csv" 
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-09.csv"   
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-08.csv"
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-07.csv"                                                          ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-06.csv" 
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-05.csv"                                                          ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-04.csv" 
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-03.csv"                                                          ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-02.csv" 
                                 ,"abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/BigData510_ProjectData/yellow_tripdata_2016-01.csv"
                                                                                             
                                                                                                                                                                                                                                                                                                                               )


// COMMAND ----------

yellowtaxicab_data.printSchema

// COMMAND ----------

// DBTITLE 1,Loading Taxi zone look up table
val TaxiZoneLookupTable = spark.read.option("inferSchema", "true").option("header",true).csv("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/Taxi_LookupTable/taxi+_zone_lookup.csv")

TaxiZoneLookupTable.show

// COMMAND ----------

TaxiZoneLookupTable.printSchema

// COMMAND ----------

// DBTITLE 1,Loading NYC Weather Data
val nyc_weatherdata = spark.read.option("inferSchema", "true").option("header",true).csv("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/nyc_weatherdata/New_York_JFK_WeatherData.csv")

nyc_weatherdata.show(10)

// COMMAND ----------

nyc_weatherdata.printSchema

// COMMAND ----------

// DBTITLE 1,Saving CSV import in Parquet format to improve performance into Container
//Saving csv files back out in Parquet, allowing it to be much faster to relaod and work with subsequently
yellowtaxicab_data.write.mode("overwrite").parquet("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/Parquet_Tables/TaxiParquet") 
TaxiZoneLookupTable.write.mode("overwrite").parquet("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/Parquet_Tables/TaxiLookupParquet") 
nyc_weatherdata.write.mode("overwrite").parquet("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/Parquet_Tables/weatherParquet") 


// COMMAND ----------

// DBTITLE 1,Creating new immutable variable for read.parquet 
val taxidata = sqlContext.read.parquet("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/Parquet_Tables/TaxiParquet")
val TaxiZone_LookupTable = sqlContext.read.parquet("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/Parquet_Tables/TaxiLookupParquet")
val weatherdata = sqlContext.read.parquet("abfss://bchan29@uwbigdatatechnologies.dfs.core.windows.net/Parquet_Tables/weatherParquet")

// COMMAND ----------

// DBTITLE 1,Persist/Cache - saving an interim result for reuse in later stages
 //persist and cache save an interim result for reuse in later stages
taxidata.persist
TaxiZone_LookupTable.persist
weatherdata.persist

// COMMAND ----------

// DBTITLE 1,Creating Temporary view table for the Summary taxi data
//Creating Temporary view table for the Summary taxi data
print("Register the DataFrame as a SQL temporary view: taxidata")
taxidata.createOrReplaceTempView("Summary_TaxiData") 


// COMMAND ----------

// DBTITLE 1,Creating Temporary view table for the taxi lookup table
//Creating Temporary view table for Taxi lookup table
print("Register the DataFrame as a SQL temporary view: taxi lookup table")
TaxiZone_LookupTable.createOrReplaceTempView("TaxiLookup_Table") 

// COMMAND ----------

// DBTITLE 1,Creating Temporary view table for NYC weather data
//Creating Temporary view table for nyc weather data
print("Register the DataFrame as a SQL temporary view: NYC weather data")
weatherdata.createOrReplaceTempView("weatherdata") 

// COMMAND ----------

// DBTITLE 1,Using RDD action "count" to return the number of taxi fares by year
//Created a User defined function to substring date column to return only the year
val substring: String => String = _.substring(0,4)
import org.apache.spark.sql.functions.udf
val substringUDF = udf(substring)

//Used the user defined function to create column called "Year"
//Used rdd transformation to filter year between 2015 to 2019. Ensuring to remove bad data.
import org.apache.spark.sql.functions._
val taxidata_v1 = yellowtaxicab_data.withColumn("Year", substringUDF($"tpep_pickup_datetime")).filter($"Year" > 2015).filter($"Year" < 2020)


val taxifare_year = taxidata_v1.groupBy("Year")
.agg(
  count($"Year").as("Number_Of_Taxifares")
)

taxifare_year.orderBy(desc("Year")).show()
 

// COMMAND ----------

// DBTITLE 1,Registering User Defined Function so I can use it in Spark SQL
//registering User Defined Function called "string_editor" in Spark SQL
spark.udf.register("string_editor", substringUDF)

// COMMAND ----------

// DBTITLE 1,Number of taxi fares by year (Visualization)
// MAGIC %sql
// MAGIC SELECT
// MAGIC Summary_Count.Year
// MAGIC ,COUNT(Summary_Count.Year) Number_Of_Taxifares
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC   SELECT
// MAGIC   string_editor(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING)) Year
// MAGIC   ,Summary_TaxiData.total_amount
// MAGIC   FROM Summary_TaxiData
// MAGIC   WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2019-12-31"
// MAGIC ) Summary_Count
// MAGIC 
// MAGIC GROUP BY
// MAGIC Summary_Count.Year
// MAGIC 
// MAGIC ORDER BY Summary_Count.Year ASC

// COMMAND ----------

// DBTITLE 1,Using Spark SQL to return cash sales by year
// MAGIC %sql
// MAGIC 
// MAGIC SELECT
// MAGIC Summary_Table.Year
// MAGIC ,SUM(Summary_Table.total_amount) CashSales
// MAGIC FROM(
// MAGIC 
// MAGIC SELECT
// MAGIC SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC ,Summary_TaxiData.total_amount
// MAGIC FROM Summary_TaxiData
// MAGIC WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2019-12-31"
// MAGIC 
// MAGIC ) as Summary_Table
// MAGIC GROUP BY
// MAGIC Summary_Table.Year
// MAGIC 
// MAGIC ORDER BY Summary_Table.Year ASC

// COMMAND ----------

// DBTITLE 1,Section 1.1 - Top 5 2019 Average Cash Sales by Pickup Location
// MAGIC %sql
// MAGIC SELECT
// MAGIC  Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC ,AVG(Average_ZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM(
// MAGIC 
// MAGIC     SELECT
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2019-01-01" AND "2019-12-31"
// MAGIC 
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC     ) Average_ZoneResults
// MAGIC     
// MAGIC     GROUP BY
// MAGIC     Average_ZoneResults.Zone
// MAGIC    ,Average_ZoneResults.Year
// MAGIC    
// MAGIC    ORDER BY AVG(Average_ZoneResults.CashSales) DESC LIMIT 5 --Top 5 Average
// MAGIC    

// COMMAND ----------

// DBTITLE 1,Section 1.2 - Top 5 2018 Average Cash Sales by Pickup Location
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC ,AVG(Average_ZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM(
// MAGIC 
// MAGIC     SELECT
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2018-01-01" AND "2018-12-31"
// MAGIC 
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC     ) Average_ZoneResults
// MAGIC     
// MAGIC     GROUP BY
// MAGIC     Average_ZoneResults.Zone
// MAGIC    ,Average_ZoneResults.Year
// MAGIC    
// MAGIC    ORDER BY AVG(Average_ZoneResults.CashSales) DESC LIMIT 5 --Top 5 Average

// COMMAND ----------

// DBTITLE 1,Section 1.3 - Top 5 2017 Average Cash Sales by Pickup Location
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC ,AVG(Average_ZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM(
// MAGIC 
// MAGIC     SELECT
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2017-01-01" AND "2017-12-31"
// MAGIC 
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC     ) Average_ZoneResults
// MAGIC     
// MAGIC     GROUP BY
// MAGIC     Average_ZoneResults.Zone
// MAGIC    ,Average_ZoneResults.Year
// MAGIC    
// MAGIC    ORDER BY AVG(Average_ZoneResults.CashSales) DESC LIMIT 5 --Top 5 Average

// COMMAND ----------

// DBTITLE 1,Section 1.4 - Top 5 2016 Average Cash Sales by Pickup Location
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC ,AVG(Average_ZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM(
// MAGIC 
// MAGIC     SELECT
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2016-12-31"
// MAGIC 
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC     ) Average_ZoneResults
// MAGIC     
// MAGIC     GROUP BY
// MAGIC     Average_ZoneResults.Zone
// MAGIC    ,Average_ZoneResults.Year
// MAGIC    
// MAGIC    ORDER BY AVG(Average_ZoneResults.CashSales) DESC LIMIT 5 --Top 5 Average

// COMMAND ----------

// DBTITLE 1,Section 2.1 - Top 5 2019 Monthly Average Cash Sales by Drop off Location
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC ,AVG(Average_ZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC     SELECT
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_dropoff_datetime BETWEEN "2019-01-01" AND "2019-12-31"
// MAGIC 
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC
// MAGIC ) Average_ZoneResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_ZoneResults.CashSales) DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 2.2 - Top 5 2018 Monthly Average Cash Sales by Drop off Location
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC ,AVG(Average_ZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC     SELECT
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_dropoff_datetime BETWEEN "2018-01-01" AND "2018-12-31"
// MAGIC 
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC
// MAGIC ) Average_ZoneResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_ZoneResults.CashSales) DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 2.3 - Top 5 2017 Monthly Average Cash Sales by Drop off Location
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC ,AVG(Average_ZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC     SELECT
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_dropoff_datetime BETWEEN "2017-01-01" AND "2017-12-31"
// MAGIC 
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC
// MAGIC ) Average_ZoneResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_ZoneResults.CashSales) DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 2.4 - Top 5 2016 Monthly Average Cash Sales by Drop off Location
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC ,AVG(Average_ZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC     SELECT
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_dropoff_datetime BETWEEN "2016-01-01" AND "2016-12-31"
// MAGIC 
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.Zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_dropoff_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC
// MAGIC ) Average_ZoneResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_ZoneResults.Zone
// MAGIC ,Average_ZoneResults.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_ZoneResults.CashSales) DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 3 - Top 5 2019 Monthly Average Cash Sales by service zones
// MAGIC %sql
// MAGIC 
// MAGIC SELECT
// MAGIC Average_ServiceZoneResults.service_zone
// MAGIC ,Average_ServiceZoneResults.Year
// MAGIC ,AVG(Average_ServiceZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC     SELECT
// MAGIC     TL.service_zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2019-01-01" AND "2019-12-31"
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.service_zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_ServiceZoneResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_ServiceZoneResults.service_zone
// MAGIC ,Average_ServiceZoneResults.Year
// MAGIC 
// MAGIC ORDER BY Average_ServiceZoneResults.service_zone DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 3.1 - Top 5 2018 Monthly Average Cash Sales by service zones
// MAGIC %sql
// MAGIC 
// MAGIC SELECT
// MAGIC Average_ServiceZoneResults.service_zone
// MAGIC ,Average_ServiceZoneResults.Year
// MAGIC ,AVG(Average_ServiceZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC     SELECT
// MAGIC     TL.service_zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2018-01-01" AND "2018-12-31"
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.service_zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_ServiceZoneResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_ServiceZoneResults.service_zone
// MAGIC ,Average_ServiceZoneResults.Year
// MAGIC 
// MAGIC ORDER BY Average_ServiceZoneResults.service_zone DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 3.2 - Top 5 2017 Monthly Average Cash Sales by service zones
// MAGIC %sql
// MAGIC 
// MAGIC SELECT
// MAGIC Average_ServiceZoneResults.service_zone
// MAGIC ,Average_ServiceZoneResults.Year
// MAGIC ,AVG(Average_ServiceZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC     SELECT
// MAGIC     TL.service_zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2017-01-01" AND "2017-12-31"
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.service_zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_ServiceZoneResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_ServiceZoneResults.service_zone
// MAGIC ,Average_ServiceZoneResults.Year
// MAGIC 
// MAGIC ORDER BY Average_ServiceZoneResults.service_zone DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 3.3 - Top 5 2016 Monthly Average Cash Sales by service zones
// MAGIC %sql
// MAGIC 
// MAGIC SELECT
// MAGIC Average_ServiceZoneResults.service_zone
// MAGIC ,Average_ServiceZoneResults.Year
// MAGIC ,AVG(Average_ServiceZoneResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC     SELECT
// MAGIC     TL.service_zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2016-12-31"
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     TL.service_zone
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_ServiceZoneResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_ServiceZoneResults.service_zone
// MAGIC ,Average_ServiceZoneResults.Year
// MAGIC 
// MAGIC ORDER BY Average_ServiceZoneResults.service_zone DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 4.1.1 - Top 5 2019 Cash Sales by passenger number
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_PassengerResults.passenger_count
// MAGIC ,Average_PassengerResults.Year
// MAGIC ,AVG(Average_PassengerResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC     SELECT
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2019-01-01" AND "2019-12-31"
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC
// MAGIC ) Average_PassengerResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_PassengerResults.passenger_count
// MAGIC ,Average_PassengerResults.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_PassengerResults.CashSales) DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 4.1.2 - 2019 Average taxi fare per fare by passenger number
// MAGIC %sql
// MAGIC 
// MAGIC 
// MAGIC     SELECT
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,AVG(Summary_TaxiData.total_amount) Average_CashSales_Per_TaxiFare
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2019-01-01" AND "2019-12-31"
// MAGIC     AND Summary_TaxiData.passenger_count IS NOT NULL
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC 
// MAGIC     ORDER BY AVG(Summary_TaxiData.total_amount) DESC

// COMMAND ----------

// DBTITLE 1,Section 4.2.1 - Top 5 2018 Cash Sales by passenger number
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_PassengerResults.passenger_count
// MAGIC ,Average_PassengerResults.Year
// MAGIC ,AVG(Average_PassengerResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC     SELECT
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2018-01-01" AND "2018-12-31"
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC
// MAGIC ) Average_PassengerResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_PassengerResults.passenger_count
// MAGIC ,Average_PassengerResults.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_PassengerResults.CashSales) DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 4.2.2 - 2018 Average taxi fare per fare by passenger number
// MAGIC %sql
// MAGIC 
// MAGIC 
// MAGIC     SELECT
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,AVG(Summary_TaxiData.total_amount) Average_CashSales_Per_TaxiFare
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2018-01-01" AND "2018-12-31"
// MAGIC     AND Summary_TaxiData.passenger_count IS NOT NULL
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC 
// MAGIC     ORDER BY AVG(Summary_TaxiData.total_amount) DESC

// COMMAND ----------

// DBTITLE 1,Section 4.3.1 - Top 5 2017 Cash Sales by passenger number
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_PassengerResults.passenger_count
// MAGIC ,Average_PassengerResults.Year
// MAGIC ,AVG(Average_PassengerResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC     SELECT
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2017-01-01" AND "2017-12-31"
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC
// MAGIC ) Average_PassengerResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_PassengerResults.passenger_count
// MAGIC ,Average_PassengerResults.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_PassengerResults.CashSales) DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 4.3.2 - 2017 Average taxi fare per fare by passenger number
// MAGIC %sql
// MAGIC 
// MAGIC 
// MAGIC     SELECT
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,AVG(Summary_TaxiData.total_amount) Average_CashSales_Per_TaxiFare
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2017-01-01" AND "2017-12-31"
// MAGIC     AND Summary_TaxiData.passenger_count IS NOT NULL
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC 
// MAGIC     ORDER BY AVG(Summary_TaxiData.total_amount) DESC

// COMMAND ----------

// DBTITLE 1,Section 4.4.1 - Top 5 2016 Cash Sales by passenger number
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_PassengerResults.passenger_count
// MAGIC ,Average_PassengerResults.Year
// MAGIC ,AVG(Average_PassengerResults.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC     SELECT
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2016-12-31"
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC     ORDER BY SUM(Summary_TaxiData.total_amount) DESC
// MAGIC ) Average_PassengerResults
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_PassengerResults.passenger_count
// MAGIC ,Average_PassengerResults.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_PassengerResults.CashSales) DESC LIMIT 5

// COMMAND ----------

// DBTITLE 1,Section 4.4.2 - 2016 Average taxi fare per fare by passenger number
// MAGIC %sql
// MAGIC 
// MAGIC 
// MAGIC     SELECT
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,AVG(Summary_TaxiData.total_amount) Average_CashSales_Per_TaxiFare
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2016-12-31"
// MAGIC     AND Summary_TaxiData.passenger_count IS NOT NULL
// MAGIC 
// MAGIC     GROUP BY
// MAGIC     Summary_TaxiData.passenger_count
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC 
// MAGIC     ORDER BY AVG(Summary_TaxiData.total_amount) DESC

// COMMAND ----------

// DBTITLE 1,Section 5.1 - Top 7 weekdays in 2019 for Monthly Average Cash Sales by Weekday and Time of day
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.Weekday
// MAGIC ,Average_Summary.TimeInterval
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,Summary_1.Weekday
// MAGIC   ,Summary_1.TimeInterval
// MAGIC   ,SUM(Summary_1.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,CASE WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 0 THEN "Monday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 1 THEN "Tuesday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 2 THEN "Wednesday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 3 THEN "Thursday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 4 THEN "Friday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 5 THEN "Saturday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 6 THEN "Sunday"
// MAGIC     ELSE "Check Conditional Column" END Weekday
// MAGIC     ,Summary_TaxiData.tpep_pickup_datetime
// MAGIC     ,CASE WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "01:00:01" AND "02:00:00" THEN "1:00:01AM-2AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "02:00:01" AND "03:00:00" THEN "2:00:01AM-3AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "03:00:01" AND "04:00:00" THEN "3:00:01AM-4AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "04:00:01" AND "05:00:00" THEN "4:00:01AM-5AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "05:00:01" AND "06:00:00" THEN "5:00:01AM-6AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "06:00:01" AND "07:00:00" THEN "6:00:01AM-7AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "07:00:01" AND "08:00:00" THEN "7:00:01AM-8AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "08:00:01" AND "09:00:00" THEN "8:00:01AM-9AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "09:00:01" AND "10:00:00" THEN "9:00:01AM-10AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "10:00:01" AND "11:00:00" THEN "10:00:01AM-11AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "11:00:01" AND "12:00:00" THEN "11:00:01AM-12PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "12:00:01" AND "13:00:00" THEN "12:00:01PM-1PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "13:00:01" AND "14:00:00" THEN "1:00:01PM-2PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "14:00:01" AND "15:00:00" THEN "2:00:01PM-3PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "15:00:01" AND "16:00:00" THEN "3:00:01PM-4PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "16:00:01" AND "17:00:00" THEN "4:00:01PM-5PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "17:00:01" AND "18:00:00" THEN "5:00:01PM-6PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "18:00:01" AND "19:00:00" THEN "6:00:01PM-7PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "19:00:01" AND "20:00:00" THEN "7:00:01PM-8PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "20:00:01" AND "21:00:00" THEN "8:00:01PM-9PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "21:00:01" AND "22:00:00" THEN "9:00:01PM-10PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "22:00:01" AND "23:00:00" THEN "10:00:01PM-11PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "23:00:01" AND "24:00:00" THEN "11:00:01PM-12PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "24:00:01" AND "01:00:00" THEN "12:00:01PM-1AM"
// MAGIC     ELSE "Check Conditional Column" END TimeInterval
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2019-01-01" AND "2019-12-31"
// MAGIC   ) Summary_1
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,Summary_1.Weekday
// MAGIC   ,Summary_1.TimeInterval
// MAGIC    ORDER BY SUM(Summary_1.CashSales) DESC
// MAGIC 
// MAGIC ) Average_Summary
// MAGIC  
// MAGIC GROUP BY 
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.Weekday
// MAGIC ,Average_Summary.TimeInterval
// MAGIC 
// MAGIC ORDER BY AVG(Average_Summary.CashSales) DESC LIMIT 100

// COMMAND ----------

// DBTITLE 1,Section 5.2 - Top 7 weekdays in 2018 for Monthly Average Cash Sales by Weekday and Time of day
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.Weekday
// MAGIC ,Average_Summary.TimeInterval
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,Summary_1.Weekday
// MAGIC   ,Summary_1.TimeInterval
// MAGIC   ,SUM(Summary_1.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,CASE WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 0 THEN "Monday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 1 THEN "Tuesday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 2 THEN "Wednesday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 3 THEN "Thursday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 4 THEN "Friday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 5 THEN "Saturday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 6 THEN "Sunday"
// MAGIC     ELSE "Check Conditional Column" END Weekday
// MAGIC     ,Summary_TaxiData.tpep_pickup_datetime
// MAGIC     ,CASE WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "01:00:01" AND "02:00:00" THEN "1:00:01AM-2AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "02:00:01" AND "03:00:00" THEN "2:00:01AM-3AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "03:00:01" AND "04:00:00" THEN "3:00:01AM-4AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "04:00:01" AND "05:00:00" THEN "4:00:01AM-5AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "05:00:01" AND "06:00:00" THEN "5:00:01AM-6AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "06:00:01" AND "07:00:00" THEN "6:00:01AM-7AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "07:00:01" AND "08:00:00" THEN "7:00:01AM-8AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "08:00:01" AND "09:00:00" THEN "8:00:01AM-9AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "09:00:01" AND "10:00:00" THEN "9:00:01AM-10AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "10:00:01" AND "11:00:00" THEN "10:00:01AM-11AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "11:00:01" AND "12:00:00" THEN "11:00:01AM-12PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "12:00:01" AND "13:00:00" THEN "12:00:01PM-1PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "13:00:01" AND "14:00:00" THEN "1:00:01PM-2PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "14:00:01" AND "15:00:00" THEN "2:00:01PM-3PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "15:00:01" AND "16:00:00" THEN "3:00:01PM-4PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "16:00:01" AND "17:00:00" THEN "4:00:01PM-5PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "17:00:01" AND "18:00:00" THEN "5:00:01PM-6PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "18:00:01" AND "19:00:00" THEN "6:00:01PM-7PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "19:00:01" AND "20:00:00" THEN "7:00:01PM-8PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "20:00:01" AND "21:00:00" THEN "8:00:01PM-9PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "21:00:01" AND "22:00:00" THEN "9:00:01PM-10PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "22:00:01" AND "23:00:00" THEN "10:00:01PM-11PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "23:00:01" AND "24:00:00" THEN "11:00:01PM-12PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "24:00:01" AND "01:00:00" THEN "12:00:01PM-1AM"
// MAGIC     ELSE "Check Conditional Column" END TimeInterval
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2018-01-01" AND "2018-12-31"
// MAGIC   ) Summary_1
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,Summary_1.Weekday
// MAGIC   ,Summary_1.TimeInterval
// MAGIC    ORDER BY SUM(Summary_1.CashSales) DESC
// MAGIC 
// MAGIC ) Average_Summary
// MAGIC  
// MAGIC GROUP BY 
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.Weekday
// MAGIC ,Average_Summary.TimeInterval
// MAGIC 
// MAGIC ORDER BY AVG(Average_Summary.CashSales) DESC LIMIT 100

// COMMAND ----------

// DBTITLE 1,Section 5.3 - Top 5 2017 Monthly Average Cash Sales by Weekday and Time of day
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.Weekday
// MAGIC ,Average_Summary.TimeInterval
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,Summary_1.Weekday
// MAGIC   ,Summary_1.TimeInterval
// MAGIC   ,SUM(Summary_1.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,CASE WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 0 THEN "Monday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 1 THEN "Tuesday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 2 THEN "Wednesday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 3 THEN "Thursday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 4 THEN "Friday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 5 THEN "Saturday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 6 THEN "Sunday"
// MAGIC     ELSE "Check Conditional Column" END Weekday
// MAGIC     ,Summary_TaxiData.tpep_pickup_datetime
// MAGIC     ,CASE WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "01:00:01" AND "02:00:00" THEN "1:00:01AM-2AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "02:00:01" AND "03:00:00" THEN "2:00:01AM-3AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "03:00:01" AND "04:00:00" THEN "3:00:01AM-4AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "04:00:01" AND "05:00:00" THEN "4:00:01AM-5AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "05:00:01" AND "06:00:00" THEN "5:00:01AM-6AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "06:00:01" AND "07:00:00" THEN "6:00:01AM-7AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "07:00:01" AND "08:00:00" THEN "7:00:01AM-8AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "08:00:01" AND "09:00:00" THEN "8:00:01AM-9AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "09:00:01" AND "10:00:00" THEN "9:00:01AM-10AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "10:00:01" AND "11:00:00" THEN "10:00:01AM-11AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "11:00:01" AND "12:00:00" THEN "11:00:01AM-12PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "12:00:01" AND "13:00:00" THEN "12:00:01PM-1PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "13:00:01" AND "14:00:00" THEN "1:00:01PM-2PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "14:00:01" AND "15:00:00" THEN "2:00:01PM-3PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "15:00:01" AND "16:00:00" THEN "3:00:01PM-4PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "16:00:01" AND "17:00:00" THEN "4:00:01PM-5PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "17:00:01" AND "18:00:00" THEN "5:00:01PM-6PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "18:00:01" AND "19:00:00" THEN "6:00:01PM-7PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "19:00:01" AND "20:00:00" THEN "7:00:01PM-8PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "20:00:01" AND "21:00:00" THEN "8:00:01PM-9PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "21:00:01" AND "22:00:00" THEN "9:00:01PM-10PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "22:00:01" AND "23:00:00" THEN "10:00:01PM-11PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "23:00:01" AND "24:00:00" THEN "11:00:01PM-12PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "24:00:01" AND "01:00:00" THEN "12:00:01PM-1AM"
// MAGIC     ELSE "Check Conditional Column" END TimeInterval
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2017-01-01" AND "2017-12-31"
// MAGIC   ) Summary_1
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,Summary_1.Weekday
// MAGIC   ,Summary_1.TimeInterval
// MAGIC    ORDER BY SUM(Summary_1.CashSales) DESC
// MAGIC 
// MAGIC ) Average_Summary
// MAGIC  
// MAGIC GROUP BY 
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.Weekday
// MAGIC ,Average_Summary.TimeInterval
// MAGIC 
// MAGIC ORDER BY AVG(Average_Summary.CashSales) DESC LIMIT 100

// COMMAND ----------

// DBTITLE 1,Section 5.4 - Top 5 2016 Monthly Average Cash Sales by Weekday and Time of day
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.Weekday
// MAGIC ,Average_Summary.TimeInterval
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,Summary_1.Weekday
// MAGIC   ,Summary_1.TimeInterval
// MAGIC   ,SUM(Summary_1.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,CASE WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 0 THEN "Monday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 1 THEN "Tuesday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 2 THEN "Wednesday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 3 THEN "Thursday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 4 THEN "Friday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 5 THEN "Saturday"
// MAGIC     WHEN WEEKDAY(Summary_TaxiData.tpep_pickup_datetime) = 6 THEN "Sunday"
// MAGIC     ELSE "Check Conditional Column" END Weekday
// MAGIC     ,Summary_TaxiData.tpep_pickup_datetime
// MAGIC     ,CASE WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "01:00:01" AND "02:00:00" THEN "1:00:01AM-2AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "02:00:01" AND "03:00:00" THEN "2:00:01AM-3AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "03:00:01" AND "04:00:00" THEN "3:00:01AM-4AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "04:00:01" AND "05:00:00" THEN "4:00:01AM-5AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "05:00:01" AND "06:00:00" THEN "5:00:01AM-6AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "06:00:01" AND "07:00:00" THEN "6:00:01AM-7AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "07:00:01" AND "08:00:00" THEN "7:00:01AM-8AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "08:00:01" AND "09:00:00" THEN "8:00:01AM-9AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "09:00:01" AND "10:00:00" THEN "9:00:01AM-10AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "10:00:01" AND "11:00:00" THEN "10:00:01AM-11AM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "11:00:01" AND "12:00:00" THEN "11:00:01AM-12PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "12:00:01" AND "13:00:00" THEN "12:00:01PM-1PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "13:00:01" AND "14:00:00" THEN "1:00:01PM-2PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "14:00:01" AND "15:00:00" THEN "2:00:01PM-3PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "15:00:01" AND "16:00:00" THEN "3:00:01PM-4PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "16:00:01" AND "17:00:00" THEN "4:00:01PM-5PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "17:00:01" AND "18:00:00" THEN "5:00:01PM-6PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "18:00:01" AND "19:00:00" THEN "6:00:01PM-7PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "19:00:01" AND "20:00:00" THEN "7:00:01PM-8PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "20:00:01" AND "21:00:00" THEN "8:00:01PM-9PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "21:00:01" AND "22:00:00" THEN "9:00:01PM-10PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "22:00:01" AND "23:00:00" THEN "10:00:01PM-11PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "23:00:01" AND "24:00:00" THEN "11:00:01PM-12PM"
// MAGIC     WHEN SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),12,8) BETWEEN "24:00:01" AND "01:00:00" THEN "12:00:01PM-1AM"
// MAGIC     ELSE "Check Conditional Column" END TimeInterval
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2016-12-31"
// MAGIC   ) Summary_1
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,Summary_1.Weekday
// MAGIC   ,Summary_1.TimeInterval
// MAGIC    ORDER BY SUM(Summary_1.CashSales) DESC
// MAGIC 
// MAGIC ) Average_Summary
// MAGIC  
// MAGIC GROUP BY 
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.Weekday
// MAGIC ,Average_Summary.TimeInterval
// MAGIC 
// MAGIC ORDER BY AVG(Average_Summary.CashSales) DESC LIMIT 100

// COMMAND ----------

// DBTITLE 1,Section 6.1 - 2019 Monthly Average Cash Sales by payment type
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_PaymentType.Payment_type
// MAGIC ,Average_PaymentType.Year
// MAGIC ,AVG(Average_PaymentType.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC   SELECT
// MAGIC   Summary_1.Payment_type
// MAGIC   ,Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,SUM(Summary_1.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     CASE WHEN Summary_TaxiData.Payment_type = "1" THEN "Credit Card"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "2" THEN "Cash"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "3" THEN "No charge"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "4" THEN "Dispute"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "5" THEN "Unknown"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "6" THEN "Voided trip"
// MAGIC     ELSE "Voided trip" END Payment_type
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2019-01-01" AND "2019-12-31"
// MAGIC   ) Summary_1    
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_1.Payment_type
// MAGIC   ,Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_1.CashSales) DESC
// MAGIC ) Average_PaymentType    
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_PaymentType.Payment_type
// MAGIC ,Average_PaymentType.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_PaymentType.CashSales) DESC LIMIT 10
// MAGIC     

// COMMAND ----------

// DBTITLE 1,Section 6.2 - 2018 Monthly Average Cash Sales by payment type
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_PaymentType.Payment_type
// MAGIC ,Average_PaymentType.Year
// MAGIC ,AVG(Average_PaymentType.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC   SELECT
// MAGIC   Summary_1.Payment_type
// MAGIC   ,Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,SUM(Summary_1.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     CASE WHEN Summary_TaxiData.Payment_type = "1" THEN "Credit Card"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "2" THEN "Cash"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "3" THEN "No charge"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "4" THEN "Dispute"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "5" THEN "Unknown"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "6" THEN "Voided trip"
// MAGIC     ELSE "Voided trip" END Payment_type
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2018-01-01" AND "2018-12-31"
// MAGIC   ) Summary_1    
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_1.Payment_type
// MAGIC   ,Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_1.CashSales) DESC
// MAGIC ) Average_PaymentType    
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_PaymentType.Payment_type
// MAGIC ,Average_PaymentType.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_PaymentType.CashSales) DESC LIMIT 10
// MAGIC     

// COMMAND ----------

// DBTITLE 1,Section 6.3 - 2017 Monthly Average Cash Sales by payment type
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_PaymentType.Payment_type
// MAGIC ,Average_PaymentType.Year
// MAGIC ,AVG(Average_PaymentType.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC   SELECT
// MAGIC   Summary_1.Payment_type
// MAGIC   ,Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,SUM(Summary_1.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     CASE WHEN Summary_TaxiData.Payment_type = "1" THEN "Credit Card"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "2" THEN "Cash"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "3" THEN "No charge"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "4" THEN "Dispute"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "5" THEN "Unknown"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "6" THEN "Voided trip"
// MAGIC     ELSE "Voided trip" END Payment_type
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2017-01-01" AND "2017-12-31"
// MAGIC   ) Summary_1    
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_1.Payment_type
// MAGIC   ,Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_1.CashSales) DESC
// MAGIC ) Average_PaymentType    
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_PaymentType.Payment_type
// MAGIC ,Average_PaymentType.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_PaymentType.CashSales) DESC LIMIT 10
// MAGIC     

// COMMAND ----------

// DBTITLE 1,Section 6.4 - 2016 Monthly Average Cash Sales by payment type
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_PaymentType.Payment_type
// MAGIC ,Average_PaymentType.Year
// MAGIC ,AVG(Average_PaymentType.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC 
// MAGIC   SELECT
// MAGIC   Summary_1.Payment_type
// MAGIC   ,Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC   ,SUM(Summary_1.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     CASE WHEN Summary_TaxiData.Payment_type = "1" THEN "Credit Card"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "2" THEN "Cash"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "3" THEN "No charge"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "4" THEN "Dispute"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "5" THEN "Unknown"
// MAGIC     WHEN Summary_TaxiData.Payment_type = "6" THEN "Voided trip"
// MAGIC     ELSE "Voided trip" END Payment_type
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     
// MAGIC     FROM Summary_TaxiData
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2016-12-31"
// MAGIC   ) Summary_1    
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_1.Payment_type
// MAGIC   ,Summary_1.Year
// MAGIC   ,Summary_1.Month
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_1.CashSales) DESC
// MAGIC ) Average_PaymentType    
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_PaymentType.Payment_type
// MAGIC ,Average_PaymentType.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_PaymentType.CashSales) DESC LIMIT 10
// MAGIC     

// COMMAND ----------

// DBTITLE 1,Section 7.1 - Top 5 2019 Monthly average Cash Sales by Least Trip Distance needed to pickup and drop off 
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,Average_Summary.trip_distance
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   TL.zone Start_Zone
// MAGIC   ,TL1.zone End_Zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC   ,Summary_TaxiData.trip_distance
// MAGIC   ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC   FROM Summary_TaxiData
// MAGIC   LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC   LEFT JOIN TaxiLookup_Table TL1 ON(TL1.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC   WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2019-01-01" AND "2019-12-31"
// MAGIC   AND Summary_TaxiData.trip_distance > 0
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   TL.zone
// MAGIC   ,TL1.zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC   ,Summary_TaxiData.trip_distance 
// MAGIC 
// MAGIC   ORDER BY Summary_TaxiData.trip_distance ASC, SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,Average_Summary.trip_distance
// MAGIC 
// MAGIC ORDER BY Average_Summary.trip_distance ASC, AVG(Average_Summary.CashSales) DESC LIMIT 10

// COMMAND ----------

// DBTITLE 1,Section 7.2 - Top 5 2018 Monthly average Cash Sales by Least Trip Distance needed to pickup and drop off 
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,Average_Summary.trip_distance
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   TL.zone Start_Zone
// MAGIC   ,TL1.zone End_Zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC   ,Summary_TaxiData.trip_distance
// MAGIC   ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC   FROM Summary_TaxiData
// MAGIC   LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC   LEFT JOIN TaxiLookup_Table TL1 ON(TL1.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC   WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2018-01-01" AND "2018-12-31"
// MAGIC   AND Summary_TaxiData.trip_distance > 0
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   TL.zone
// MAGIC   ,TL1.zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC   ,Summary_TaxiData.trip_distance 
// MAGIC 
// MAGIC   ORDER BY Summary_TaxiData.trip_distance ASC, SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,Average_Summary.trip_distance
// MAGIC 
// MAGIC ORDER BY Average_Summary.trip_distance ASC, AVG(Average_Summary.CashSales) DESC LIMIT 10

// COMMAND ----------

// DBTITLE 1,Section 7.3 - Top 5 2017 Monthly average Cash Sales by Least Trip Distance needed to pickup and drop off 
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,Average_Summary.trip_distance
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   TL.zone Start_Zone
// MAGIC   ,TL1.zone End_Zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC   ,Summary_TaxiData.trip_distance
// MAGIC   ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC   FROM Summary_TaxiData
// MAGIC   LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC   LEFT JOIN TaxiLookup_Table TL1 ON(TL1.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC   WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2017-01-01" AND "2017-12-31"
// MAGIC   AND Summary_TaxiData.trip_distance > 0
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   TL.zone
// MAGIC   ,TL1.zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC   ,Summary_TaxiData.trip_distance 
// MAGIC 
// MAGIC   ORDER BY Summary_TaxiData.trip_distance ASC, SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,Average_Summary.trip_distance
// MAGIC 
// MAGIC ORDER BY Average_Summary.trip_distance ASC, AVG(Average_Summary.CashSales) DESC LIMIT 10

// COMMAND ----------

// DBTITLE 1,Section 7.4 - Top 5 2016 Monthly average Cash Sales by Least Trip Distance needed to pickup and drop off 
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,Average_Summary.trip_distance
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   TL.zone Start_Zone
// MAGIC   ,TL1.zone End_Zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC   ,Summary_TaxiData.trip_distance
// MAGIC   ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC   FROM Summary_TaxiData
// MAGIC   LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC   LEFT JOIN TaxiLookup_Table TL1 ON(TL1.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC   WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2016-12-31"
// MAGIC   AND Summary_TaxiData.trip_distance > 0
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   TL.zone
// MAGIC   ,TL1.zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC   ,Summary_TaxiData.trip_distance 
// MAGIC 
// MAGIC   ORDER BY Summary_TaxiData.trip_distance ASC, SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,Average_Summary.trip_distance
// MAGIC 
// MAGIC ORDER BY Average_Summary.trip_distance ASC, AVG(Average_Summary.CashSales) DESC LIMIT 10

// COMMAND ----------

// DBTITLE 1,Section 8.1 - Top 5 2019 Monthly Average Cash sales based on best route
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   TL.zone Start_Zone
// MAGIC   ,TL1.zone End_Zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC   ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC   FROM Summary_TaxiData
// MAGIC   LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC   LEFT JOIN TaxiLookup_Table TL1 ON(TL1.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC   WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2019-01-01" AND "2019-12-31"
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   TL.zone
// MAGIC   ,TL1.zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_Summary.CashSales) DESC LIMIT 10

// COMMAND ----------

// DBTITLE 1,Section 8.2 - Top 5 2018 Monthly Average Cash sales based on best route
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   TL.zone Start_Zone
// MAGIC   ,TL1.zone End_Zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC   ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC   FROM Summary_TaxiData
// MAGIC   LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC   LEFT JOIN TaxiLookup_Table TL1 ON(TL1.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC   WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2018-01-01" AND "2018-12-31"
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   TL.zone
// MAGIC   ,TL1.zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_Summary.CashSales) DESC LIMIT 10

// COMMAND ----------

// DBTITLE 1,Section 8.3 - Top 5 2017 Monthly Average Cash sales based on best route
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   TL.zone Start_Zone
// MAGIC   ,TL1.zone End_Zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC   ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC   FROM Summary_TaxiData
// MAGIC   LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC   LEFT JOIN TaxiLookup_Table TL1 ON(TL1.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC   WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2017-01-01" AND "2017-12-31"
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   TL.zone
// MAGIC   ,TL1.zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_Summary.CashSales) DESC LIMIT 10

// COMMAND ----------

// DBTITLE 1,Section 8.4 - Top 5 2016 Monthly Average Cash sales based on best route
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   TL.zone Start_Zone
// MAGIC   ,TL1.zone End_Zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC   ,SUM(Summary_TaxiData.total_amount) CashSales
// MAGIC   FROM Summary_TaxiData
// MAGIC   LEFT JOIN TaxiLookup_Table TL ON(TL.LocationID = Summary_TaxiData.PULocationID)
// MAGIC   LEFT JOIN TaxiLookup_Table TL1 ON(TL1.LocationID = Summary_TaxiData.DOLocationID)
// MAGIC 
// MAGIC   WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2016-12-31"
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   TL.zone
// MAGIC   ,TL1.zone
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4)
// MAGIC   ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2)
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_TaxiData.total_amount) DESC 
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Start_Zone
// MAGIC ,Average_Summary.End_Zone
// MAGIC ,Average_Summary.Year
// MAGIC 
// MAGIC ORDER BY AVG(Average_Summary.CashSales) DESC LIMIT 10

// COMMAND ----------

// DBTITLE 1,Section 9.1 - Top 5 2019 Monthly Average Cash Sales by Weather Type
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.WeatherType
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   Summary_Data.Year
// MAGIC   ,Summary_Data.Month
// MAGIC   ,CASE WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow > 0 THEN "Snowy & Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow = 0 THEN "Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation = 0 AND Summary_Data.NewSnow > 0 THEN "Snowy Day"
// MAGIC   ELSE "Normal Day" END WeatherType
// MAGIC   ,SUM(Summary_Data.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,Summary_TaxiData.tpep_pickup_datetime Date
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     ,weatherdata.Precipitation Precipitation_Old
// MAGIC     ,weatherdata.NewSnow Newsnow_Old
// MAGIC     ,CAST(CASE WHEN weatherdata.Precipitation = "T" THEN 0.01
// MAGIC     ELSE weatherdata.Precipitation END AS FLOAT) Precipitation 
// MAGIC     ,CAST(CASE WHEN weatherdata.NewSnow = "T" THEN 0.01
// MAGIC     WHEN weatherdata.NewSnow = "M" THEN 0.01
// MAGIC     ELSE weatherdata.NewSnow END AS FLOAT) NewSnow
// MAGIC 
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN weatherdata ON(SUBSTR(CAST(weatherdata.Date AS STRING),1,10) = SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,10) )
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2019-01-01" AND "2019-12-31"
// MAGIC   ) Summary_Data
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_Data.Year
// MAGIC   ,Summary_Data.Month
// MAGIC   ,CASE WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow > 0 THEN "Snowy & Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow = 0 THEN "Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation = 0 AND Summary_Data.NewSnow > 0 THEN "Snowy Day"
// MAGIC   ELSE "Normal Day" END
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_Data.CashSales) DESC
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.WeatherType
// MAGIC 
// MAGIC ORDER BY SUM(Average_Summary.CashSales) DESC 

// COMMAND ----------

// DBTITLE 1,Section 9.2 - Top 5 2018 Monthly Average Cash Sales by Weather Type
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.WeatherType
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   Summary_Data.Year
// MAGIC   ,Summary_Data.Month
// MAGIC   ,CASE WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow > 0 THEN "Snowy & Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow = 0 THEN "Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation = 0 AND Summary_Data.NewSnow > 0 THEN "Snowy Day"
// MAGIC   ELSE "Normal Day" END WeatherType
// MAGIC   ,SUM(Summary_Data.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,Summary_TaxiData.tpep_pickup_datetime Date
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     ,weatherdata.Precipitation Precipitation_Old
// MAGIC     ,weatherdata.NewSnow Newsnow_Old
// MAGIC     ,CAST(CASE WHEN weatherdata.Precipitation = "T" THEN 0.01
// MAGIC     ELSE weatherdata.Precipitation END AS FLOAT) Precipitation 
// MAGIC     ,CAST(CASE WHEN weatherdata.NewSnow = "T" THEN 0.01
// MAGIC     WHEN weatherdata.NewSnow = "M" THEN 0.01
// MAGIC     ELSE weatherdata.NewSnow END AS FLOAT) NewSnow
// MAGIC 
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN weatherdata ON(SUBSTR(CAST(weatherdata.Date AS STRING),1,10) = SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,10) )
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2018-01-01" AND "2018-12-31"
// MAGIC   ) Summary_Data
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_Data.Year
// MAGIC   ,Summary_Data.Month
// MAGIC   ,CASE WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow > 0 THEN "Snowy & Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow = 0 THEN "Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation = 0 AND Summary_Data.NewSnow > 0 THEN "Snowy Day"
// MAGIC   ELSE "Normal Day" END
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_Data.CashSales) DESC
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.WeatherType
// MAGIC 
// MAGIC ORDER BY SUM(Average_Summary.CashSales) DESC 

// COMMAND ----------

// DBTITLE 1,Section 9.3 - Top 5 2017 Monthly Average Cash Sales by Weather Type
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.WeatherType
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   Summary_Data.Year
// MAGIC   ,Summary_Data.Month
// MAGIC   ,CASE WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow > 0 THEN "Snowy & Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow = 0 THEN "Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation = 0 AND Summary_Data.NewSnow > 0 THEN "Snowy Day"
// MAGIC   ELSE "Normal Day" END WeatherType
// MAGIC   ,SUM(Summary_Data.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,Summary_TaxiData.tpep_pickup_datetime Date
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     ,weatherdata.Precipitation Precipitation_Old
// MAGIC     ,weatherdata.NewSnow Newsnow_Old
// MAGIC     ,CAST(CASE WHEN weatherdata.Precipitation = "T" THEN 0.01
// MAGIC     ELSE weatherdata.Precipitation END AS FLOAT) Precipitation 
// MAGIC     ,CAST(CASE WHEN weatherdata.NewSnow = "T" THEN 0.01
// MAGIC     WHEN weatherdata.NewSnow = "M" THEN 0.01
// MAGIC     ELSE weatherdata.NewSnow END AS FLOAT) NewSnow
// MAGIC 
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN weatherdata ON(SUBSTR(CAST(weatherdata.Date AS STRING),1,10) = SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,10) )
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2017-01-01" AND "2017-12-31"
// MAGIC   ) Summary_Data
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_Data.Year
// MAGIC   ,Summary_Data.Month
// MAGIC   ,CASE WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow > 0 THEN "Snowy & Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow = 0 THEN "Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation = 0 AND Summary_Data.NewSnow > 0 THEN "Snowy Day"
// MAGIC   ELSE "Normal Day" END
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_Data.CashSales) DESC
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.WeatherType
// MAGIC 
// MAGIC ORDER BY SUM(Average_Summary.CashSales) DESC 

// COMMAND ----------

// DBTITLE 1,Section 9.4 - Top 5 2016 Monthly Average Cash Sales by Weather Type
// MAGIC %sql
// MAGIC SELECT
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.WeatherType
// MAGIC ,AVG(Average_Summary.CashSales) Monthly_Average_CashSales
// MAGIC FROM
// MAGIC (
// MAGIC   SELECT
// MAGIC   Summary_Data.Year
// MAGIC   ,Summary_Data.Month
// MAGIC   ,CASE WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow > 0 THEN "Snowy & Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow = 0 THEN "Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation = 0 AND Summary_Data.NewSnow > 0 THEN "Snowy Day"
// MAGIC   ELSE "Normal Day" END WeatherType
// MAGIC   ,SUM(Summary_Data.CashSales) CashSales
// MAGIC   FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC     SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,4) Year
// MAGIC     ,SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),6,2) Month
// MAGIC     ,Summary_TaxiData.tpep_pickup_datetime Date
// MAGIC     ,Summary_TaxiData.total_amount CashSales
// MAGIC     ,weatherdata.Precipitation Precipitation_Old
// MAGIC     ,weatherdata.NewSnow Newsnow_Old
// MAGIC     ,CAST(CASE WHEN weatherdata.Precipitation = "T" THEN 0.01
// MAGIC     ELSE weatherdata.Precipitation END AS FLOAT) Precipitation 
// MAGIC     ,CAST(CASE WHEN weatherdata.NewSnow = "T" THEN 0.01
// MAGIC     WHEN weatherdata.NewSnow = "M" THEN 0.01
// MAGIC     ELSE weatherdata.NewSnow END AS FLOAT) NewSnow
// MAGIC 
// MAGIC     FROM Summary_TaxiData
// MAGIC     LEFT JOIN weatherdata ON(SUBSTR(CAST(weatherdata.Date AS STRING),1,10) = SUBSTR(CAST(Summary_TaxiData.tpep_pickup_datetime AS STRING),1,10) )
// MAGIC 
// MAGIC     WHERE Summary_TaxiData.tpep_pickup_datetime BETWEEN "2016-01-01" AND "2016-12-31"
// MAGIC   ) Summary_Data
// MAGIC 
// MAGIC   GROUP BY
// MAGIC   Summary_Data.Year
// MAGIC   ,Summary_Data.Month
// MAGIC   ,CASE WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow > 0 THEN "Snowy & Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation > 0 AND Summary_Data.NewSnow = 0 THEN "Rainy Day"
// MAGIC   WHEN Summary_Data.Precipitation = 0 AND Summary_Data.NewSnow > 0 THEN "Snowy Day"
// MAGIC   ELSE "Normal Day" END
// MAGIC 
// MAGIC   ORDER BY SUM(Summary_Data.CashSales) DESC
// MAGIC ) Average_Summary
// MAGIC 
// MAGIC GROUP BY
// MAGIC Average_Summary.Year
// MAGIC ,Average_Summary.WeatherType
// MAGIC 
// MAGIC ORDER BY SUM(Average_Summary.CashSales) DESC 
