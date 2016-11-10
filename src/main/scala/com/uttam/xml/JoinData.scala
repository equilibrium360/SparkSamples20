package com.uttam.xml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, IntegerType, StringType, StructType}

/**
  * Created by UNIVERSE on 11/9/16.
  *
  * Join Dataframes on Multiple Keys.
  * Set Broadcast Join
  */
object JoinData {

  def main(args: Array[String]) = {

    val ss = SparkSession.builder()
      .appName("JoinData")
      .master("local")
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


    val userSchema = StructType(Array(
      StructField("userid", IntegerType, true),
      StructField("age", StringType, true),
      StructField("gender", StringType, true),
      StructField("occupation", StringType, true),
      StructField("zipcode", StringType, true)))


    val userDF = ss.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("delimiter", "|")
      .schema(userSchema)
      .load("datafiles/movie/users.txt")

      val joinedData = ratingDF.join(userDF,(userDF("userid") === ratingDF("userid")),"LeftOuter" )

    joinedData.show()




  }

}
