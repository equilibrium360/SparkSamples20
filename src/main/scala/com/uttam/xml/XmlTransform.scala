package com.uttam.xml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created by UNIVERSE on 12/2/16.
  * Transform XML
  * Move corresAddress to Address level
  */

object XmlTransform {

  def main(args : Array[String]) = {

    val ss = SparkSession.builder()
      .appName("TestXML")
      .master("local")
      .getOrCreate()


    import ss.implicits._

    //Specifying Schema  is optional
    val customSchema = StructType(Array(
      StructField("_id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("reviews", StructType(List(StructField("review", ArrayType(StringType), nullable=true))))))


    val xmlDF = ss.read.format("com.databricks.spark.xml")
      .option("rowTag", "book").load("datafiles/books.xml")

    xmlDF.printSchema()

    println(xmlDF.schema)

    //Explode Function
    val flatten =  xmlDF.toDF().select($"author", $"genre", $"title",
      explode($"reviews.review").as("reviews_flat"))



    //Perform aggregation on single column
    //To
    flatten.groupBy($"genre", $"title").agg(collect_list($"author")).show();






  }


}
