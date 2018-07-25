import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object Cook4j {

  def main(args: Array[String]) {

    val logger = Logger.getLogger(getClass().getName())

    logger.info("Creating spark session...")
    val spark = SparkSession.builder.appName("Cook4j Application").getOrCreate()

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

    logger.info("As part of Data Model - Writing in parquet format for recipe table...")
    val df1 = df.select("recipe_id","recipe_name","description").distinct().orderBy(asc("recipe_id"))
    df1.write.format("parquet").mode("Overwrite").option("compression","gzip").save("/mnt/wesley/customer/recipe/")

    logger.info("As part of Data Model - Writing in parquet format for ingredients table...")
    val df2 = df.select("recipe_id","ingredient","active").distinct().orderBy(asc("recipe_id"))
    df2.write.format("parquet").mode("Overwrite").option("compression","gzip").save("/mnt/wesley/customer/ingedients/")

    val df3 = df.alias("a").groupBy("created_hours","recipe_id").agg(count("*").alias("recipe_count_per_hour")).orderBy("created_hours").orderBy("recipe_id").join(df1.alias("b"),col("a.recipe_id") === col("b.recipe_id")).select(col("created_hours"),col("a.recipe_id"),col("recipe_name"),col("recipe_count_per_hour"))
    df3.show()

    logger.info("Query tranformation - To extract average recipe updates per hour...")
    val df4 = df3.groupBy("recipe_id").agg(avg("recipe_count_per_hour").alias("avg_recipe_count_per_hour")).orderBy("recipe_id").join(df1,df3.col("recipe_id")=== df1.col("recipe_id")).select("recipe_name","avg_recipe_count_per_hour")
    df4.show()

    logger.info("Query tranformation - To extract recipe updates at 10 a.m. ...")
    val df5 = df3.filter("created_hours = 10").select("recipe_name","recipe_count_per_hour")
    df5.show()

    spark.stop()
  }

}
