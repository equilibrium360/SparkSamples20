package com.uttam.xml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.functions._
import  com.uttam.xml.udaf.CustomAgg

/**
  * Created by UNIVERSE on 11/10/16.
  */
object UDAFSample {


  def main(args : Array[String]) = {

    val ss = SparkSession.builder()
      .appName("UDAF in Spark")
      .master("local[*]")
      .getOrCreate()

    val ratingSchema = StructType(Array(
      StructField("userid", IntegerType, true),
      StructField("itemid", StringType, true),
      StructField("rating", StringType, true),
      StructField("timestamp", StringType, true)))


    val ratingDF = ss.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("delimiter", "\t")
      .schema(ratingSchema)
      .load("datafiles/movie/ratings.txt")

    val customAgg = new CustomAgg()


    //Add column
    val ratingTrans1 = ratingDF.withColumn("ratinginfo",  struct(ratingDF("itemid"),ratingDF("rating"),ratingDF("timestamp")))

    val ratingTrans2 = ratingTrans1.select("userid", "ratinginfo").toDF().repartition(5)

    ratingTrans2.printSchema()

    val results = ratingTrans2.groupBy(ratingTrans2("userid")).agg(customAgg(ratingTrans2("ratinginfo")).as("gp"))

    results.write.json("datafiles/groupByoutput/")

    //result.foreach(r => print(r.toString()))






  }

}
