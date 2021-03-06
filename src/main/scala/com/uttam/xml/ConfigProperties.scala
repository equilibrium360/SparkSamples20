package com.uttam.xml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

/**
  * Created by UNIVERSE on 11/20/16.
  *
  *
  */

/*
* create a properties file and specify while submitting application to spark
*
* Syntax
*
* spark-submit --properties-file prop.properties ..........
* */
object ConfigProperties {

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
      StructField("uid", IntegerType, true),
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

    val joinedData = ratingDF.join(userDF,(userDF("uid") === ratingDF("userid")), "inner" )

    joinedData.groupBy("userid").agg(max("age"), count("uid"), count("age")).show()

    // joinedData.show()


  }



}
