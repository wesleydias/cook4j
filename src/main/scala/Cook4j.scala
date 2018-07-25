import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object Cook4j {

  def main(args: Array[String]) {

    val logger = Logger.getLogger(getClass().getName())

    logger.info("Creating spark session...")
    val spark = SparkSession.builder.appName("Cook4j Application").getOrCreate()

        /* 
    Assumptions
    ----------
    The data received every hour of 1 TB, and is full load. Hence all write operations uses "Overwrite" mode.
    Incase of Incremental load the Mode will change to "Append".
    */  

    logger.info("Reading raw csv file...")
    val read_raw = spark.read.option("header","true").option("inferSchema","true").csv("/FileStore/tables/recipe_inged.csv")

    val ts1 = to_timestamp(col("created_date"), "MM/dd/yy HH:mm")
    val ts2 = hour(col("created_ts"))
    val ts3 = to_timestamp(col("updated_date"), "MM/dd/yy HH:mm")

    logger.info("Writing in parquet format for performance optimization with gzip compression...")
    val write_pq = read_raw.withColumn("created_ts", ts1).withColumn("created_hours", ts2).withColumn("updated_date", ts3)
    write_pq.write.format("parquet").mode("Overwrite").option("compression","gzip").partitionBy("created_hours").save("/mnt/wesley/customer/walmart/")

    logger.info("Reading in parquet format for query analysis...")
    val df=spark.read.load("/mnt/wesley/customer/walmart/")

    /* 
    Data Modal
    ----------
    The Raw CSV data is split into 2 tables which can be queried by Front-end API.

    Recipe 
    ------------
    Schema{
    recipe_id,
    recipe_name,
    description
    }

    Ingredients
    -------------
    Schema{
    recipe_id,
    ingredient,
    active
    }

    This should satisfy the conditions where the user can navigates from recipe summary page(hence refrencing only recipe dataset) 
    to detailed recipe page onclick (referncing the Ingredients dataset) for any given recipe_id and also this same table can be used 
    to refrence back all the recipe using this ingedients
    */  

    logger.info("As part of Data Model - Writing in parquet format for recipe table...")
    val df1 = df.select("recipe_id","recipe_name","description").distinct().orderBy(asc("recipe_id"))
    df1.write.format("parquet").mode("Overwrite").option("compression","gzip").save("/mnt/wesley/customer/recipe/")

    logger.info("As part of Data Model - Writing in parquet format for ingredients table...")
    val df2 = df.select("recipe_id","ingredient","active").distinct().orderBy(asc("recipe_id"))
    df2.write.format("parquet").mode("Overwrite").option("compression","gzip").save("/mnt/wesley/customer/ingedients/")

    /* 
    Persistence store
    -----------------
    For the purpose of testing of this code hdfs is used. The raw data is partitioned by created_time_hour as most of the queries 
    have the hour as the main predicate in focus as you will see below. Hence on doing a explain plain on the dataframe you can notice that it makes
    sense to partition our data on Created_time_hours.

    */  

    val df3 = df.alias("a").groupBy("created_hours","recipe_id").agg(count("*").alias("recipe_count_per_hour")).orderBy("created_hours").orderBy("recipe_id").join(df1.alias("b"),col("a.recipe_id") === col("b.recipe_id")).select(col("created_hours"),col("a.recipe_id"),col("recipe_name"),col("recipe_count_per_hour"))
    df3.show()

    /* 
    Analytical query 1
    -----------------
    To extract average recipe updates per hour.

    Sample output
    -------------
    +--------------------+-------------------------+
    |         recipe_name|avg_recipe_count_per_hour|
    +--------------------+-------------------------+
    |Acorn Squash and ...|                      3.0|
    |            Affogato|                      5.0|
    |Almond Butter Oat...|                      6.0|
    |Almond Malted Bri...|                      3.0|
    |  Almond Orange Cake|                      3.0|
    |       Alphabet Soup|                      1.0|
    |Apple Ginger Komb...|                      7.0|
    +--------------------+-------------------------+

    */  

    logger.info("Query tranformation - To extract average recipe updates per hour...")
    val df4 = df3.groupBy("recipe_id").agg(avg("recipe_count_per_hour").alias("avg_recipe_count_per_hour")).orderBy("recipe_id").join(df1,df3.col("recipe_id")=== df1.col("recipe_id")).select("recipe_name","avg_recipe_count_per_hour")
    df4.show()

    /* 
    Analytical query 2
    -----------------
    To extract number of recipes updates at 10 a.m.

    Sample output
    -------------
    +--------------------+-------------+---------------------+
    |         recipe_name|created_hours|recipe_count_per_hour|
    +--------------------+-------------+---------------------+
    |  Parker House Rolls|           10|                    3|
    |Parmesan Chicken ...|           10|                   21|
    |Parmesan Chicken ...|           10|                  123|
    |Parmesan Peas wit...|           10|                   13|
    |Pasta, Chicken & ...|           10|                   38|
    |Paul’s Thanksgivi...|           10|                   45|
    |        Peach Butter|           10|                   16|
    +--------------------+-------------+---------------------+
    */  

    logger.info("Query tranformation - To extract recipe updates at 10 a.m. ...")
    val df5 = df3.filter("created_hours = 10").select("recipe_name","recipe_count_per_hour")
    df5.show()

    spark.stop()
  }

}
