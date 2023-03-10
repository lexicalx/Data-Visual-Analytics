// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// LOAD THE "taxi_zone_lookup.csv" FILE SIMILARLY AS ABOVE. CAST ANY COLUMN TO APPROPRIATE DATA TYPE IF NECESSARY.
val customSchema_lookup = StructType(Array(StructField("LocationID", IntegerType, true), StructField("Borough", StringType, true), StructField("Zone", StringType, true), StructField("service_zone", StringType, true)))
val df_lookup = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema_lookup)
   .load("/FileStore/tables/taxi_zone_lookup.csv") // the csv file which you want to work with
// ENTER THE CODE BELOW

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can comment the df.show(5) and uncomment display(df) to see the data differently. You will find these two functions useful in reporting your results.
// display(df)
df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)
df_filter.show(5)

// COMMAND ----------

// PART 1a: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Output Schema: DOLocationID int, number_of_dropoffs int 

// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW

val k2 = df_filter.groupBy("DOLocationID").agg(count("DOLocationID").as("number_of_dropoffs"))
k2.orderBy(col("number_of_dropoffs").desc, col("DOLocationID").asc).show(5)

// COMMAND ----------

// PART 1b: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 
// Output Schema: PULocationID int, number_of_pickups int

// Hint: Code is very similar to part 1a above.

// ENTER THE CODE BELOW
val k3 = df_filter.groupBy("PULocationID").agg(count("PULocationID").as("number_of_pickups"))
k3.orderBy(col("number_of_pickups").desc, col("PULocationID").asc).show(5)

// COMMAND ----------

// PART 2: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Output Schema: LocationID int, number_activities int

// Hint: In order to get the result, you may need to perform a join operation between the two dataframes that you created in earlier parts (to come up with the sum of the number of pickups and dropoffs on each location). 

// ENTER THE CODE BELOW
val k4 = k2.join(k3,k2("DOLocationID")===k3("PULocationID"))
var k5 = k4.select("DOLocationID", "number_of_dropoffs", "number_of_pickups").withColumn("number_activities", col("number_of_dropoffs") + col("number_of_pickups"))
var k6 = k5.select(col("DOLocationID").alias("LocationID"), col("number_activities")).orderBy(col("number_activities").desc)
k6.show(3)

// COMMAND ----------

// PART 3: List all the boroughs in the order of having the highest to lowest number of activities (i.e. sum of all pickups and all dropoffs at that LocationID), along with the total number of activity counts for each borough in NYC during that entire period of time.
// Output Schema: Borough string, total_number_activities int

// Hint: You can use the dataframe obtained from the previous part, and will need to do the join with the 'taxi_zone_lookup' dataframe. Also, checkout the "agg" function applied to a grouped dataframe.

// ENTER THE CODE BELOW
var k7 = k6.join(df_lookup, k6("LocationID") === df_lookup("LocationID"))
// df_lookup.show(5)
var k8 = k7.groupBy("Borough").agg(sum("number_activities").as("total_number_activities")).orderBy(col("total_number_activities").desc)
k8.show()

// COMMAND ----------

// PART 4: List the top 2 days of week with the largest number of (daily) average pickups, along with the values of average number of pickups on each of the two days. The day of week should be a string with its full name, for example, "Monday" - not a number 1 or "Mon" instead.
// Output Schema: day_of_week string, avg_count float

// Hint: You may need to group by the "date" (without time stamp - time in the day) first. Checkout "to_date" function.
// import org.apache.spark.sql.functions.{col, to_date}
// ENTER THE CODE BELOW
// df_filter.show(5)
var k9 = df_filter.withColumn("pickup_datetime",to_date(col("pickup_datetime")))
var k10 = k9.groupBy("pickup_datetime").agg(count("pickup_datetime").as("count"))
var k11 = k10.withColumn("day_of_week", date_format(col("pickup_datetime"), "EEEE"))
var k12 = k11.groupBy("day_of_week").agg(avg("count").as("avg_count"))
k12.orderBy(col("avg_count").desc).show(2)

// COMMAND ----------

// PART 5: For each particular hour of a day (0 to 23, 0 being midnight) - in their order from 0 to 23, find the zone in Brooklyn borough with the LARGEST number of pickups. 
// Output Schema: hour_of_day int, zone string, max_count int

// Hint: You may need to use "Window" over hour of day, along with "group by" to find the MAXIMUM count of pickups

// ENTER THE CODE BELOW
// df_filter.show(5)
var k13 = df_filter.join(df_lookup, df_filter("PULocationID") === df_lookup("LocationID"))
var k14 = k13.filter(k13("Borough")==="Brooklyn")
var k15 = k14.withColumn("hour_of_day", hour(col("pickup_datetime")))
var k16 = k15.groupBy("hour_of_day", "Zone").agg(count("hour_of_day").as("max_count")).orderBy(col("hour_of_day").asc)
var k17 = k16.withColumn("orderdNumberForDay",
    row_number()
    .over(
      Window.orderBy(col("max_count").desc)
        .partitionBy("hour_of_day")
    )
).filter(col("orderdNumberForDay") === lit(1))
 .select("hour_of_day", "Zone", "max_count")
k17.show(24,truncate=false)

// COMMAND ----------

// PART 6 - Find which 3 different days of the January, in Manhattan, saw the largest percentage increment in pickups compared to previous day, in the order from largest increment % to smallest increment %. 
// Print the day of month along with the percent CHANGE (can be negative), rounded to 2 decimal places, in number of pickups compared to previous day.
// Output Schema: day int, percent_change float


// Hint: You might need to use lag function, over a window ordered by day of month.

// ENTER THE CODE BELOW


var k18 = df_filter.join(df_lookup, df_filter("PULocationID") === df_lookup("LocationID"))
var k19 = k18.filter(k13("Borough")==="Manhattan")
var k20 = k19.withColumn("month", month(col("pickup_datetime")))
var k21 = k20.filter(k20("month")===1)
var k22 = k21.withColumn("day", dayofmonth(col("pickup_datetime")))
var k23 = k22.groupBy("day").agg(count("day").as("day_count")).orderBy(col("day").asc)
val w = Window.orderBy("day")
var k24 = k23.withColumn("prev_day_count", lag("day_count", 1, 1769).over(w))
var k25 = k24.withColumn("percent_change", (col("day_count") - col("prev_day_count"))*100.0 / col("prev_day_count"))
var k26 = k25.orderBy(col("percent_change").desc).limit(3)
var k27 = k26.select("day", "percent_change")
var k28 = k27.withColumn("percent_change", round(col("percent_change"),2))
k28.show()


// COMMAND ----------

k24.show(31)

// COMMAND ----------

k25.show(31)

// COMMAND ----------


