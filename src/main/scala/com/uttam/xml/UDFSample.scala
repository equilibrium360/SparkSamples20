package com.uttam.xml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

/**
  * Created by UNIVERSE on 11/10/16.
  */
object UDFSample {

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

    def getItemCode(curCode: String):String ={

      curCode+" Update String"

    }

    import org.apache.spark.sql.functions.udf
    val getItemCodeUDF = udf((curCode: String) => {

      curCode + "Update String"
    })


    ratingDF.withColumn("NewCol", getItemCodeUDF(ratingDF("itemid")) ).show()


    //Another way of adding udf as regular function
    val itemlbl = udf(getItemIdLabel(_:String))

    ratingDF.withColumn("ItemLabel", itemlbl(ratingDF("itemid")) ).show()






  }

  // regular spark function
  def getItemIdLabel(curId: String):String = {

    return "ItemLabel"

  }



}
